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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.runner.application.ApplicationHandler;
import org.apache.flink.runtime.dispatcher.runner.application.EmbeddedApplicationExecutor;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.entrypoint.application.ProgramUtils;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class YarnApplicationClusterEntrypoint extends ClusterEntrypoint {

	public static final JobID ZERO_JOB_ID = new JobID(0, 0);

	private final ApplicationHandler applicationSubmitter;

	public YarnApplicationClusterEntrypoint(
			final Configuration configuration,
			final ApplicationHandler applicationSubmitter) {
		super(configuration);
		this.applicationSubmitter = checkNotNull(applicationSubmitter);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory
				.createApplicationComponentFactory(YarnResourceManagerFactory.getInstance(), applicationSubmitter);
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(Configuration configuration, ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore(); // TODO: 30.01.20 think about the fault-tolerance in this case.
	}

	public static void main(String[] args) throws IOException, ProgramInvocationException {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnApplicationClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Map<String, String> env = System.getenv();

		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
				workingDirectory != null,
				"Working directory variable (%s) not set",
				ApplicationConstants.Environment.PWD.key());

		try {
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}

		final Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env);
		final PackagedProgram executable = ProgramUtils.getExecutable(configuration, env);
		configuration.set(DeploymentOptions.TARGET, EmbeddedApplicationExecutor.NAME);
		configuration.set(DeploymentOptions.ATTACHED, true);

		final JobID jobID = createJobIdForCluster(configuration);
		final EmbeddedApplicationSubmitter applicationSubmitter =
				new EmbeddedApplicationSubmitter(jobID, configuration, executable);

		final YarnApplicationClusterEntrypoint yarnApplicationClusterEntrypoint =
				new YarnApplicationClusterEntrypoint(configuration, applicationSubmitter);

		ClusterEntrypoint.runClusterEntrypoint(yarnApplicationClusterEntrypoint);
	}

	private static JobID createJobIdForCluster(Configuration globalConfiguration) {
		if (HighAvailabilityMode.isHighAvailabilityModeActivated(globalConfiguration)) {
			return ZERO_JOB_ID;
		} else {
			return JobID.generate();
		}
	}
}
