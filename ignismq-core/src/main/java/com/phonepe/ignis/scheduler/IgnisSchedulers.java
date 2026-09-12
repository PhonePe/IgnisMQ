/**
 * Copyright (c) 2026 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.ignis.scheduler;

import com.phonepe.ignis.utils.Constants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The two pools ignisMQ schedules on, and the isolation between them.
 *
 * <table>
 *   <caption>Why one pool was not enough</caption>
 *   <tr><th></th><th>Control</th><th>Worker</th></tr>
 *   <tr><td>Tasks</td><td>queue watcher, sweeper</td><td>consumers, shovels</td></tr>
 *   <tr><td>Count</td><td>exactly two, per process</td><td>unbounded: queues x concurrency</td></tr>
 *   <tr><td>Runs</td><td>ignisMQ's own code</td><td>arbitrary user handlers</td></tr>
 *   <tr><td>Sizing</td><td>fixed</td><td>grows to {@link Constants#SCHEDULER_MAX_THREADS}</td></tr>
 * </table>
 *
 * Sharing one pool coupled those two columns: enough slow or backlogged consumers and the watcher
 * stops refreshing queues and the sweeper stops recovering orphans, silently. Both are the control
 * plane - the watcher is how a queue created elsewhere becomes consumable here, and the sweeper is
 * the only thing that rescues messages claimed by a dead consumer. Neither may be starved by the
 * data plane it supervises.
 * <p>
 * Shovels share the worker pool deliberately. They are per-queue, configurable in concurrency and
 * do real storage I/O, so they are the same class of workload as consumers, not the same class as
 * the watcher. A third pool would add threads and a knob without buying isolation that matters.
 * <p>
 * The split bounds the blast radius of a badly behaved handler; it does not make the worker pool
 * immune to one. That is what the consumer run budget is for.
 */
@Slf4j
public final class IgnisSchedulers {

    @Getter
    private final IgnisSchedulerCommands control;
    @Getter
    private final IgnisSchedulerCommands worker;

    public IgnisSchedulers() {
        this.control = new IgnisSchedulerCommands("ignismq-control",
                Constants.SCHEDULER_CONTROL_THREADS, Constants.SCHEDULER_CONTROL_THREADS);
        this.worker = new IgnisSchedulerCommands("ignismq-worker",
                Constants.SCHEDULER_BASE_THREADS, Constants.SCHEDULER_MAX_THREADS);
    }

    /**
     * Stops both pools, workers first.
     * <p>
     * Order matters: the watcher creates and scales consumers, so stopping the control pool last
     * means a refresh cannot register new work into a pool that is already shutting down.
     *
     * @return true when both pools stopped within their grace period.
     */
    public boolean stop() {
        final boolean workersStopped = worker.stop();
        final boolean controlStopped = control.stop();
        if (!workersStopped || !controlStopped) {
            log.warn("Scheduler shutdown incomplete: workers stopped={}, control stopped={}",
                    workersStopped, controlStopped);
        }
        return workersStopped && controlStopped;
    }

    public boolean isStopped() {
        return worker.isStopped() && control.isStopped();
    }
}
