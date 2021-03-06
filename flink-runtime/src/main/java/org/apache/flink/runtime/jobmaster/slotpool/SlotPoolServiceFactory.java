/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nonnull;

/** Factory interface for {@link SlotPoolService}. */
public interface SlotPoolServiceFactory {

    @Nonnull
    SlotPoolService createSlotPoolService(@Nonnull JobID jobId);

    static SlotPoolServiceFactory fromConfiguration(Configuration configuration, JobType jobType) {
        final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);
        final Time slotIdleTimeout =
                Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_IDLE_TIMEOUT));
        final Time batchSlotTimeout =
                Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));

        if (ClusterOptions.isDeclarativeResourceManagementEnabled(configuration)) {
            if (ClusterOptions.isDeclarativeSchedulerEnabled(configuration)
                    && jobType == JobType.STREAMING) {
                return new DeclarativeSlotPoolServiceFactory(
                        SystemClock.getInstance(), slotIdleTimeout, rpcTimeout);
            }

            return new DeclarativeSlotPoolBridgeServiceFactory(
                    SystemClock.getInstance(), rpcTimeout, slotIdleTimeout, batchSlotTimeout);
        } else {
            return new DefaultSlotPoolServiceFactory(
                    SystemClock.getInstance(), rpcTimeout, slotIdleTimeout, batchSlotTimeout);
        }
    }
}
