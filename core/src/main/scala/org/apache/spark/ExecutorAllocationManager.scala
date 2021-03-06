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

package org.apache.spark

import scala.collection.mutable

import org.apache.spark.scheduler._

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The add policy depends on whether there are backlogged tasks waiting to be scheduled. If
 * the scheduler queue is not drained in N seconds, then new executors are added. If the queue
 * persists for another M seconds, then more executors are added and so on. The number added
 * in each round increases exponentially from the previous round until an upper bound on the
 * number of executors has been reached.
 *
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 *
 * The remove policy is simpler: If an executor has been idle for K seconds, meaning it has not
 * been scheduled to run any tasks, then it is removed.
 *
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor has been idle for this duration, remove it
 */
private[spark] class ExecutorAllocationManager(sc: SparkContext) extends Logging {
  import ExecutorAllocationManager._

  private val conf = sc.conf

  // Lower and upper bounds on the number of executors. These are required.
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", -1)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", -1)
  verifyBounds()

  // How long there must be backlogged tasks for before an addition is triggered
  private val schedulerBacklogTimeout = conf.getLong(
    "spark.dynamicAllocation.schedulerBacklogTimeout", 60)

  // Same as above, but used only after `schedulerBacklogTimeout` is exceeded
  private val sustainedSchedulerBacklogTimeout = conf.getLong(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", schedulerBacklogTimeout)

  // How long an executor must be idle for before it is removed
  private val removeThresholdSeconds = conf.getLong(
    "spark.dynamicAllocation.executorIdleTimeout", 600)

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // Number of executors that have been requested but have not registered yet
  private var numExecutorsPending = 0

  // Executors that have been requested to be removed but have not been killed yet
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // All known executors
  private val executorIds = new mutable.HashSet[String]

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  private var addTime: Long = NOT_SET

  // A timestamp for each executor of when the executor should be removed, indexed by the ID
  // This is set when an executor is no longer running a task, or when it first registers
  private val removeTimes = new mutable.HashMap[String, Long]

  // Polling loop interval (ms)
  private val intervalMillis: Long = 100

  // Whether we are testing this class. This should only be used internally.
  private val testing = conf.getBoolean("spark.dynamicAllocation.testing", false)

  // Clock used to schedule when executors should be added and removed
  private var clock: Clock = new RealClock

  /**
   * Verify that the lower and upper bounds on the number of executors are valid.
   * If not, throw an appropriate exception.
   */
  private def verifyBounds(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be set!")
    }
    if (minNumExecutors == 0 || maxNumExecutors == 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"spark.dynamicAllocation.minExecutors ($minNumExecutors) must " +
        s"be less than or equal to spark.dynamicAllocation.maxExecutors ($maxNumExecutors)!")
    }
  }

  /**
   * Use a different clock for this allocation manager. This is mainly used for testing.
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors.
   */
  def start(): Unit = {
    val listener = new ExecutorAllocationListener(this)
    sc.addSparkListener(listener)
    startPolling()
  }

  /**
   * Start the main polling thread that keeps track of when to add and remove executors.
   */
  private def startPolling(): Unit = {
    val t = new Thread {
      override def run(): Unit = {
        while (true) {
          try {
            schedule()
          } catch {
            case e: Exception => logError("Exception in dynamic executor allocation thread!", e)
          }
          Thread.sleep(intervalMillis)
        }
      }
    }
    t.setName("spark-dynamic-executor-allocation")
    t.setDaemon(true)
    t.start()
  }

  /**
   * If the add time has expired, request new executors and refresh the add time.
   * If the remove time for an existing executor has expired, kill the executor.
   * This is factored out into its own method for testing.
   */
  private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis
    if (addTime != NOT_SET && now >= addTime) {
      addExecutors()
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeout seconds)")
      addTime += sustainedSchedulerBacklogTimeout * 1000
    }

    removeTimes.foreach { case (executorId, expireTime) =>
      if (now >= expireTime) {
        removeExecutor(executorId)
        removeTimes.remove(executorId)
      }
    }
  }

  /**
   * Request a number of executors from the cluster manager.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   * Return the number actually requested.
   */
  private def addExecutors(): Int = synchronized {
    // Do not request more executors if we have already reached the upper bound
    val numExistingExecutors = executorIds.size + numExecutorsPending
    if (numExistingExecutors >= maxNumExecutors) {
      logDebug(s"Not adding executors because there are already ${executorIds.size} " +
        s"registered and $numExecutorsPending pending executor(s) (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }

    // Request executors with respect to the upper bound
    val actualNumExecutorsToAdd =
      if (numExistingExecutors + numExecutorsToAdd <= maxNumExecutors) {
        numExecutorsToAdd
      } else {
        maxNumExecutors - numExistingExecutors
      }
    val newTotalExecutors = numExistingExecutors + actualNumExecutorsToAdd
    val addRequestAcknowledged = testing || sc.requestExecutors(actualNumExecutorsToAdd)
    if (addRequestAcknowledged) {
      logInfo(s"Requesting $actualNumExecutorsToAdd new executor(s) because " +
        s"tasks are backlogged (new desired total will be $newTotalExecutors)")
      numExecutorsToAdd =
        if (actualNumExecutorsToAdd == numExecutorsToAdd) numExecutorsToAdd * 2 else 1
      numExecutorsPending += actualNumExecutorsToAdd
      actualNumExecutorsToAdd
    } else {
      logWarning(s"Unable to reach the cluster manager " +
        s"to request $actualNumExecutorsToAdd executors!")
      0
    }
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is received.
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to remove unknown executor $executorId!")
      return false
    }

    // Do not kill the executor again if it is already pending to be killed (should never happen)
    if (executorsPendingToRemove.contains(executorId)) {
      logWarning(s"Attempted to remove executor $executorId " +
        s"when it is already pending to be removed!")
      return false
    }

    // Do not kill the executor if we have already reached the lower bound
    val numExistingExecutors = executorIds.size - executorsPendingToRemove.size
    if (numExistingExecutors - 1 < minNumExecutors) {
      logInfo(s"Not removing idle executor $executorId because there are only " +
        s"$numExistingExecutors executor(s) left (limit $minNumExecutors)")
      return false
    }

    // Send a request to the backend to kill this executor
    val removeRequestAcknowledged = testing || sc.killExecutor(executorId)
    if (removeRequestAcknowledged) {
      logInfo(s"Removing executor $executorId because it has been idle for " +
        s"$removeThresholdSeconds seconds (new desired total will be ${numExistingExecutors - 1})")
      executorsPendingToRemove.add(executorId)
      true
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor $executorId!")
      false
    }
  }

  /**
   * Callback invoked when the specified executor has been added.
   */
  private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      executorIds.foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
      if (numExecutorsPending > 0) {
        numExecutorsPending -= 1
        logDebug(s"Decremented number of pending executors ($numExecutorsPending left)")
      }
    } else {
      logWarning(s"Duplicate executor $executorId has registered")
    }
  }

  /**
   * Callback invoked when the specified executor has been removed.
   */
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)
      removeTimes.remove(executorId)
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Executor $executorId is no longer pending to " +
          s"be removed (${executorsPendingToRemove.size} left)")
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Callback invoked when the scheduler receives new pending tasks.
   * This sets a time in the future that decides when executors should be added
   * if it is not already set.
   */
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeout seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeout * 1000
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug(s"Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle.
   */
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    if (!removeTimes.contains(executorId) && !executorsPendingToRemove.contains(executorId)) {
      logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
        s"scheduled to run on the executor (to expire in $removeThresholdSeconds seconds)")
      removeTimes(executorId) = clock.getTimeMillis + removeThresholdSeconds * 1000
    }
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor.
   */
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    removeTimes.remove(executorId)
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener. For simplicity, it does not account
   * for speculated tasks.
   */
  private class ExecutorAllocationListener(allocationManager: ExecutorAllocationManager)
    extends SparkListener {

    private val stageIdToNumTasks = new mutable.HashMap[Int, Int]
    private val stageIdToTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
    private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      synchronized {
        val stageId = stageSubmitted.stageInfo.stageId
        val numTasks = stageSubmitted.stageInfo.numTasks
        stageIdToNumTasks(stageId) = numTasks
        allocationManager.onSchedulerBacklogged()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      synchronized {
        val stageId = stageCompleted.stageInfo.stageId
        stageIdToNumTasks -= stageId
        stageIdToTaskIndices -= stageId

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        if (stageIdToNumTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
      val stageId = taskStart.stageId
      val taskId = taskStart.taskInfo.taskId
      val taskIndex = taskStart.taskInfo.index
      val executorId = taskStart.taskInfo.executorId

      // If this is the last pending task, mark the scheduler queue as empty
      stageIdToTaskIndices.getOrElseUpdate(stageId, new mutable.HashSet[Int]) += taskIndex
      val numTasksScheduled = stageIdToTaskIndices(stageId).size
      val numTasksTotal = stageIdToNumTasks.getOrElse(stageId, -1)
      if (numTasksScheduled == numTasksTotal) {
        // No more pending tasks for this stage
        stageIdToNumTasks -= stageId
        if (stageIdToNumTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }

      // Mark the executor on which this task is scheduled as busy
      executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
      allocationManager.onExecutorBusy(executorId)
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      val executorId = taskEnd.taskInfo.executorId
      val taskId = taskEnd.taskInfo.taskId

      // If the executor is no longer running scheduled any tasks, mark it as idle
      if (executorIdToTaskIds.contains(executorId)) {
        executorIdToTaskIds(executorId) -= taskId
        if (executorIdToTaskIds(executorId).isEmpty) {
          executorIdToTaskIds -= executorId
          allocationManager.onExecutorIdle(executorId)
        }
      }
    }

    override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
      val executorId = blockManagerAdded.blockManagerId.executorId
      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
        allocationManager.onExecutorAdded(executorId)
      }
    }

    override def onBlockManagerRemoved(
        blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
      allocationManager.onExecutorRemoved(blockManagerRemoved.blockManagerId.executorId)
    }
  }

}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}

/**
 * An abstract clock for measuring elapsed time.
 */
private trait Clock {
  def getTimeMillis: Long
}

/**
 * A clock backed by a monotonically increasing time source.
 * The time returned by this clock does not correspond to any notion of wall-clock time.
 */
private class RealClock extends Clock {
  override def getTimeMillis: Long = System.nanoTime / (1000 * 1000)
}

/**
 * A clock that allows the caller to customize the time.
 * This is used mainly for testing.
 */
private class TestClock(startTimeMillis: Long) extends Clock {
  private var time: Long = startTimeMillis
  override def getTimeMillis: Long = time
  def tick(ms: Long): Unit = { time += ms }
}
