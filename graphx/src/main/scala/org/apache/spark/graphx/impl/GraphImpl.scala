/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.impl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl._
import org.apache.spark.graphx.util.BytecodeUtils
import org.apache.spark.Logging


/**
 * An implementation of [[org.apache.spark.graphx.Graph]] to support computation on graphs.
 *
 * Graphs are represented using two RDDs: `vertices`, which contains vertex attributes and the
 * routing information for shipping vertex attributes to edge partitions, and
 * `replicatedVertexView`, which contains edges and the vertex attributes mentioned by each edge.
 */
class GraphImpl[VD: ClassTag, ED: ClassTag] protected (
    @transient val vertices: VertexRDD[VD],
    @transient val replicatedVertexView: ReplicatedVertexView[VD, ED])
  extends Graph[VD, ED] with Serializable with Logging  {

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  @transient override val edges: EdgeRDDImpl[ED, VD] = replicatedVertexView.edges

  /** Return a RDD that brings edges together with their source and destination vertices. */
  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    replicatedVertexView.upgrade(vertices, true, true)
    replicatedVertexView.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    })
  }

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    replicatedVertexView.edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = {
    vertices.cache()
    replicatedVertexView.edges.cache()
    this
  }

  override def checkpoint(): Unit = {
    vertices.checkpoint()
    replicatedVertexView.edges.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && replicatedVertexView.edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, replicatedVertexView.edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None => Seq()
    }
  }

  override def unpersist(blocking: Boolean = true): Graph[VD, ED] = {
    unpersistVertices(blocking)
    replicatedVertexView.edges.unpersist(blocking)
    this
  }

  override def unpersistVertices(blocking: Boolean = true): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    // TODO: unpersist the replicated vertices in `replicatedVertexView` but leave the edges alone
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    partitionBy(partitionStrategy, edges.partitions.size)
  }
   
    override def partitionBy(
    partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[VD, ED] = {
    println("numPartitions=" + numPartitions)
    val startTime = System.currentTimeMillis
    val ThreshHold: Int = 70
    // strategy
    val edTag = classTag[ED]
    val vdTag = classTag[VD]

    
    class Data()  extends  scala.Serializable{
      var outgoing : List[Edge[(ED, PartitionID)]] = null
      var id : VertexId = -1
      var partners : List[VertexId] = null
      var majorPartition : PartitionID = -1
      var selectedEdge : Edge[(ED, PartitionID)] = null
      var partnerOuts : 
         Array[(VertexId, Iterable[Edge[(ED, PartitionID)]])] = null
      var edgeToExchange :  Edge[(ED, PartitionID)] = null
    }
    
    var newEdges = partitionStrategy match {
      case PartitionStrategy.JaBeJaVcDominant => {
        val V = vertices.collect
        
        
        var initEdges = edges.mapValues(x => (x.attr,
          PartitionStrategy.RandomVertexCut
          .getPartition(x.srcId, x.dstId, numPartitions)))
        
        // take first N
        val N = numPartitions * 10
        
        val Nsteps = V.size / N
        
        val totalSwaps = vertices.context.accumulator(0)


        for (i <- 0 until Nsteps / 100) {
          logError(s"step $i from $Nsteps")

          val t0 = System.currentTimeMillis()
          val Nvertices = (for (i <- 0 until N)
            yield V.apply(
            (math.random * V.length).toInt % V.length)).map(x => x._1)
            .toSet
 
          val t1 = System.currentTimeMillis()
          // map add outgoing edges
          val out = initEdges.filter(
              x => Nvertices.contains(x.srcId))
              .groupBy(edge => edge.srcId).collect

          val t2 = System.currentTimeMillis()
          
          var data = Nvertices.map(x => {
            var XX = new Data()
            XX.id = x
            for (a <- out) {
              if (a._1 == x) {
                XX.outgoing = a._2.toList
                val randomVertex =
                  V.apply((V.length * math.random).toInt % V.length)._1
                XX.partners = a._2.map(x => x.dstId).toList :+ randomVertex
              }
            }
            XX
          })
          
          val t3 = System.currentTimeMillis()

          // filter  not internal
          data = data.filter(x => {
            if (x.outgoing != null) {
              (x.outgoing.map(a => a.attr._2).toSet.size > 1
                && x.partners != null)
            } else {
              false
            }
          })

          val t4 = System.currentTimeMillis()
          
          // map add selectedEdge
          data = data.map(x => {
            val ot = x.outgoing
              .groupBy(a => a.attr._2)
              .toList
              .sortWith((a, b) => a._2.size < b._2.size)
            if (ot.length > 1) {
              if (ot.head._2.size < ot.last._2.size) {
                x.majorPartition = ot.last._2.head.attr._2
                x.selectedEdge = ot.head._2.head
              }
            }
            x
          })
          
          val t5 = System.currentTimeMillis()

          // and filter if it does not have 'selectedEdge'
          data = data.filter(x => {
            x.selectedEdge != null
          })
          
          val t6 = System.currentTimeMillis()

          // map add Major Partition

          // map add partners
          // map add outgoing edges to all neighbours (filter triplets)
          val allPartners = data.flatMap(x => x.partners)
          
          val t7 = System.currentTimeMillis()
          
          val outForPatners = initEdges.
              filter(x => allPartners.contains(x.srcId)).groupBy(x => x.srcId)
              
          data = data.map(x => {
            val partners = x.partners.toSet
            x.partnerOuts = outForPatners.filter(q => partners.contains(q._1)).collect
            x
          })

          val t8 = System.currentTimeMillis()
          
          // map and first candidate to excachange
          data = data.map(x => {
            val ofp = x.partnerOuts

            var notFound = true
            for (p <- ofp if notFound) {
              val o = p._2
                .groupBy(x => x.attr._2)
                .toList
                .sortWith((a, b) => a._2.size < b._2.size)

              val head = o.head
              val last = o.last

              if (head._2.size < last._2.size) {
                val majorPartition = x.majorPartition
                if (head._2.head.attr._2 == majorPartition) {
                  val selectedEdge = x.selectedEdge
                  if (last._2.head.attr._2 == selectedEdge.attr._2) {
                    // we found edge to exchange
                    notFound = false
                    x.edgeToExchange = head._2.head
                  }
                }
              }

            }
            x
          })

          val t9 = System.currentTimeMillis()
          
          data = data.filter(x => {
            x.edgeToExchange != null
          })

          val t10 = System.currentTimeMillis()
          
          // ? check the all selected and outgoind were different
          val pairs = data.flatMap(x => {
            List(x.edgeToExchange, x.selectedEdge)
          }).toList
          val unique = pairs.filter(i => pairs.indexOf(i) == pairs.lastIndexOf(i)).toSet
          
          val t11 = System.currentTimeMillis()

          // leave only unique
          data = data.filter(x => {
            val exchange = x.edgeToExchange
            val selectedEdge = x.selectedEdge
            unique.contains(exchange) && unique.contains(selectedEdge)
          })
          
          val t12 = System.currentTimeMillis()

          val swaps = vertices.context.accumulator(0)

          initEdges = initEdges.mapValues(x => {
            var xx = x
            var notFound = true
            for (d <- data) {
              if (x.srcId == d.selectedEdge.srcId
                && x.dstId == d.selectedEdge.dstId
                && x.attr._2 == d.selectedEdge.attr._2) {
                xx.attr = (xx.attr._1, d.edgeToExchange.attr._2)
                notFound = false
                swaps += 1
                totalSwaps += 1
              } else if (x.srcId == d.edgeToExchange.srcId
                && x.dstId == d.edgeToExchange.dstId
                && x.attr._2 == d.edgeToExchange.attr._2) {
                xx.attr = (xx.attr._1, d.selectedEdge.attr._2)
                notFound = false
                swaps += 1
                totalSwaps += 1
              }
            }
            xx.attr
          })
          initEdges.count

          val t13 = System.currentTimeMillis()
          
          val a = Array(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13)
          
          for(i <- 1 until a.length){
            val t = a(i) - a(i-1)
            logError(s"T$i : " + t)
          }
          
          logError("swaped " + swaps.value)

        }
        
        logError("TOTAL swaped: " + totalSwaps.value)

        edges.withPartitionsRDD(initEdges.map { e =>
          (e.attr._2, (e.srcId, e.dstId, e.attr._1))
        }
          .partitionBy(new HashPartitioner(numPartitions))
          .mapPartitionsWithIndex({ (pid, iter) =>
            val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
            iter.foreach { message =>
              val data = message._2
              builder.add(data._1, data._2, data._3)
            }
            val edgePartition = builder.toEdgePartition
            Iterator((pid, edgePartition))
          }, preservesPartitioning = true)).cache()
      }

      // it's actually HybridCut plus Edge2D (Grid)
      case PartitionStrategy.HybridCutPlus => {
        println("HybridCutPlus")
        // val inDegrees: VertexRDD[Int] = this.inDegrees
        val LookUpTable0 = edges.map(e => (e.dstId, (e.srcId, e.attr)))
          .join(this.degrees.map(e => (e._1, e._2)))
        // (dstId, ( (srcId, attr), (Total Degree Count of dst) ))
        val LookUpTable1 = LookUpTable0.map(e => (e._2._1._1, (e._1, e._2._1._2,
          e._2._2))).join(this.degrees.map(e => (e._1, e._2)))
        // (srcID, ((dstId, attr, dstDegreeCount), srcDegreeCount) )

        edges.withPartitionsRDD(LookUpTable1.map { e =>

          var part: PartitionID = 0
          val srcId = e._1
          val dstId = e._2._1._1
          val attr = e._2._1._2
          val srcDegreeCount = e._2._2
          val dstDegreeCount = e._2._1._3
          val numParts = numPartitions

          val mixingPrime: VertexId = 1125899906842597L
          var flag: Boolean = true
          // high high : 2D
          if (srcDegreeCount > ThreshHold && dstDegreeCount > ThreshHold) {
            part = PartitionStrategy.EdgePartition2D.getPartition(srcId, dstId, numPartitions)
            flag = false
          }
          // high low : Low
          if (flag && srcDegreeCount > ThreshHold) {
            part = ((math.abs(dstId) * mixingPrime) % numParts).toInt
            flag = false
          }
          // low high : Low
          if (flag && dstDegreeCount > ThreshHold) {
            part = ((math.abs(srcId) * mixingPrime) % numParts).toInt
            flag = false
          }
          // low low : 2D
          if (flag) {
            part = PartitionStrategy.EdgePartition2D.getPartition(srcId, dstId, numPartitions)
          }

          // Should we be using 3-tuple or an optimized class
          // new MessageToPartition(part, (srcId, dstId, attr))
          (part, (srcId, dstId, attr))
        }
          .partitionBy(new HashPartitioner(numPartitions))
          .mapPartitionsWithIndex({ (pid, iter) =>
            val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
            iter.foreach { message =>
              val data = message._2
              builder.add(data._1, data._2, data._3)
            }
            val edgePartition = builder.toEdgePartition
            Iterator((pid, edgePartition))
          }, preservesPartitioning = true)).cache()
      }

      case PartitionStrategy.HybridCut => {
        println("HybridCut")
        // val inDegrees: VertexRDD[Int] = this.inDegrees
        val LookUpTable = edges.map(e => (e.dstId, (e.srcId, e.attr)))
          .join(this.inDegrees.map(e => (e._1, e._2)))
          .partitionBy(new HashPartitioner(numPartitions))
        val ret = edges.withPartitionsRDD(LookUpTable.map { e =>

          var part: PartitionID = 0
          val srcId = e._2._1._1
          val dstId = e._1
          val attr = e._2._1._2
          val DegreeCount = e._2._2
          val numParts = numPartitions

          val mixingPrime: VertexId = 1125899906842597L
          // val DegreeCount : Int = inDegrees.lookup(dst).head
          if (DegreeCount > ThreshHold) {
            // high-cut
            // hash code
            //part = (math.abs(srcId).hashCode % numParts).toInt
            part = ((math.abs(srcId) * mixingPrime) % numParts).toInt
          } else {
            // low-cut
            // hash code
            //part = (math.abs(dstId).hashCode % numParts).toInt
            part = ((math.abs(dstId) * mixingPrime) % numParts).toInt
          }

          // Should we be using 3-tuple or an optimized class
          // new MessageToPartition(part, (srcId, dstId, attr))
          (part, (srcId, dstId, attr))
        }
          .partitionBy(new HashPartitioner(numPartitions))
          .mapPartitionsWithIndex({ (pid, iter) =>
            val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
            iter.foreach { message =>
              val data = message._2
              builder.add(data._1, data._2, data._3)
            }
            val edgePartition = builder.toEdgePartition
            Iterator((pid, edgePartition))
          }, preservesPartitioning = true)).cache()
        LookUpTable.unpersist()
        ret
      }
      case _ => {
        val x = edges.map { e =>
          val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
          //val part: PartitionID = partitionStrategy.getPartition(srcId, dstId, numPartitions)

          // Should we be using 3-tuple or an optimized class
          (part, (e.srcId, e.dstId, e.attr))
          //(part, (srcId, dstId, attr))
        }
        edges.withPartitionsRDD(
          x
            .partitionBy(new HashPartitioner(numPartitions))
            .mapPartitionsWithIndex({ (pid, iter) =>
              val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
              iter.foreach { message =>
                val data = message._2
                builder.add(data._1, data._2, data._3)
              }
              val edgePartition = builder.toEdgePartition
              Iterator((pid, edgePartition))
            }, preservesPartitioning = true)).cache()
      }
    }

    logInfo("It took %d ms to partition".format(System.currentTimeMillis - startTime))
    // println("It took %d ms to partition".format(System.currentTimeMillis - startTime))

    GraphImpl.fromExistingRDDs(vertices.withEdges(newEdges), newEdges)
  }
  

  override def reverse: Graph[VD, ED] = {
    new GraphImpl(vertices.reverseRoutingTables(), replicatedVertexView.reverse())
  }

  override def mapVertices[VD2: ClassTag]
    (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(_.map(f)).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      GraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
  }

  override def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def mapTriplets[ED2: ClassTag](
      f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
      tripletFields: TripletFields): Graph[VD, ED2] = {
    vertices.cache()
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val newEdges = replicatedVertexView.edges.mapEdgePartitions { (pid, part) =>
      part.map(f(pid, part.tripletIterator(tripletFields.useSrc, tripletFields.useDst)))
    }
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = x => true,
      vpred: (VertexId, VD) => Boolean = (a, b) => true): Graph[VD, ED] = {
    vertices.cache()
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
    // Filter the triplets. We must always upgrade the triplet view fully because vpred always runs
    // on both src and dst vertices
    replicatedVertexView.upgrade(vertices, true, true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def mask[VD2: ClassTag, ED2: ClassTag] (
      other: Graph[VD2, ED2]): Graph[VD, ED] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    val newEdges = replicatedVertexView.edges.mapEdgePartitions(
      (pid, part) => part.groupEdges(merge))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  override def mapReduceTriplets[A: ClassTag](
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]): VertexRDD[A] = {

    def sendMsg(ctx: EdgeContext[VD, ED, A]) {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }

    val mapUsesSrcAttr = accessesVertexAttr(mapFunc, "srcAttr")
    val mapUsesDstAttr = accessesVertexAttr(mapFunc, "dstAttr")
    val tripletFields = new TripletFields(mapUsesSrcAttr, mapUsesDstAttr, true)

    aggregateMessagesWithActiveSet(sendMsg, reduceFunc, tripletFields, activeSetOpt)
  }

  override def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]): VertexRDD[A] = {

    vertices.cache()
    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val view = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.withActiveSet(activeSet)
      case None =>
        replicatedVertexView
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    val preAgg = view.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, edgePartition) =>
        // Choose scan method
        val activeFraction = edgePartition.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
        activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            }
          case Some(EdgeDirection.Either) =>
            // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
            // the index here. Instead we have to scan all edges and then do the filter.
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            }
          case Some(EdgeDirection.In) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.DstOnly)
          case _ => // None
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Neither)
        }
    }).setName("GraphImpl.aggregateMessages - preAgg")

    // do the final reduction reusing the index map
    vertices.aggregateUsingIndex(preAgg, mergeMsg)
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
      (other: RDD[(VertexId, U)])
      (updateF: (VertexId, VD, Option[U]) => VD2)
      (implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    // The implicit parameter eq will be populated by the compiler if VD and VD2 are equal, and left
    // null if not
    if (eq != null) {
      vertices.cache()
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      GraphImpl(newVerts, replicatedVertexView.edges)
    }
  }

  /** Test whether the closure accesses the the attribute with name `attrName`. */
  private def accessesVertexAttr(closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }
} // end of class GraphImpl


object GraphImpl {

  /** Create a graph from edges, setting referenced vertices to `defaultVertexAttr`. */
  def apply[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    fromEdgeRDD(EdgeRDD.fromEdges(edges), defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /** Create a graph from EdgePartitions, setting referenced vertices to `defaultVertexAttr`. */
  def fromEdgePartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    fromEdgeRDD(EdgeRDD.fromEdgePartitions(edgePartitions), defaultVertexAttr, edgeStorageLevel,
      vertexStorageLevel)
  }

  /** Create a graph from vertices and edges, setting missing vertices to `defaultVertexAttr`. */
  def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    val edgeRDD = EdgeRDD.fromEdges(edges)(classTag[ED], classTag[VD])
      .withTargetStorageLevel(edgeStorageLevel).cache()
    val vertexRDD = VertexRDD(vertices, edgeRDD, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel).cache()
    GraphImpl(vertexRDD, edgeRDD)
  }

  /**
   * Create a graph from a VertexRDD and an EdgeRDD with arbitrary replicated vertices. The
   * VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
   * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
   */
  def apply[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED] = {
    // Convert the vertex partitions in edges to the correct type
    val newEdges = edges.asInstanceOf[EdgeRDDImpl[ED, _]]
      .mapEdgePartitions((pid, part) => part.withoutVertexAttributes[VD])
    GraphImpl.fromExistingRDDs(vertices, newEdges)
  }

  /**
   * Create a graph from a VertexRDD and an EdgeRDD with the same replicated vertex type as the
   * vertices. The VertexRDD must already be set up for efficient joins with the EdgeRDD by calling
   * `VertexRDD.withEdges` or an appropriate VertexRDD constructor.
   */
  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED] = {
    new GraphImpl(vertices, new ReplicatedVertexView(edges.asInstanceOf[EdgeRDDImpl[ED, VD]]))
  }

  /**
   * Create a graph from an EdgeRDD with the correct vertex type, setting missing vertices to
   * `defaultVertexAttr`. The vertices will have the same number of partitions as the EdgeRDD.
   */
  private def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edges: EdgeRDDImpl[ED, VD],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    val edgesCached = edges.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices = VertexRDD.fromEdges(edgesCached, edgesCached.partitions.size, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices, edgesCached)
  }

} // end of object GraphImpl
