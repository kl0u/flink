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

package org.apache.flink.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedApplicationExecutorFactory implements PipelineExecutorFactory {

	private final Collection<JobID> applicationJobIds;

	private final Collection<JobID> recoveredJobIds;

	private final DispatcherGateway dispatcherGateway;

	public EmbeddedApplicationExecutorFactory(
			final Collection<JobID> applicationJobIds,
			final Collection<JobID> recoveredJobIds,
			final DispatcherGateway dispatcherGateway) {
		this.applicationJobIds = requireNonNull(applicationJobIds);
		this.recoveredJobIds = requireNonNull(recoveredJobIds);
		this.dispatcherGateway = requireNonNull(dispatcherGateway);
	}

	@Override
	public String getName() {
		return EmbeddedApplicationExecutor.NAME;
	}

	@Override
	public boolean isCompatibleWith(final Configuration configuration) {
		return true;
	}

	@Override
	public PipelineExecutor getExecutor(final Configuration configuration) {
		requireNonNull(configuration);

		final JobID jobId = getOrCreateJobId(configuration);
		this.applicationJobIds.add(jobId);

		final EmbeddedClient client = new EmbeddedClient(jobId, configuration, dispatcherGateway);
		return new EmbeddedApplicationExecutor(recoveredJobIds, jobId, client);
	}

	private static JobID getOrCreateJobId(final Configuration configuration) {
		requireNonNull(configuration);

		return configuration
				.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
				.map(JobID::fromHexString)
				.orElse(createJobIdForCluster(configuration));
	}

	/**
	 * This method assumes that there is only one job in the application, and in the case of HA
	 * it assigns the {@link JobID#ZERO_JOB_ID}.
	 * todo this has to change in the future and maybe throw an exception.
	 */
	private static JobID createJobIdForCluster(Configuration configuration) {
		if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
			return JobID.ZERO_JOB_ID;
		} else {
			return JobID.generate();
		}
	}
}
