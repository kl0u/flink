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
import org.apache.flink.application.ApplicationDispatcherFactory;
import org.apache.flink.application.ApplicationDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.application.EmbeddedApplicationExecutor;
import org.apache.flink.application.EmbeddedApplicationHandler;
import org.apache.flink.application.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.entrypoint.application.JarFilePackagedProgramRetriever;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.util.ClusterEntrypointUtils.tryFindUserLibDirectory;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Javadoc.
 */
@Internal
public class YarnApplicationClusterEntryPoint extends ClusterEntrypoint {

	public YarnApplicationClusterEntryPoint(final Configuration configuration) {
		super(configuration);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(final Configuration configuration) throws Exception {
		final PackagedProgramRetriever retriever = new JarFilePackagedProgramRetriever(configuration, getUsrLibDir(configuration));
		final PackagedProgram executable = retriever.getPackagedProgram();

		final EmbeddedApplicationHandler applicationHandler =
				new EmbeddedApplicationHandler(configuration, executable);

		return new DefaultDispatcherResourceManagerComponentFactory(
				new DefaultDispatcherRunnerFactory(
						ApplicationDispatcherLeaderProcessFactoryFactory
								.create(ApplicationDispatcherFactory.INSTANCE, applicationHandler)),
				YarnResourceManagerFactory.getInstance(),
				JobRestEndpointFactory.INSTANCE);
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
			final Configuration configuration,
			final ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore();
	}

	@Nullable
	private static File getUsrLibDir(final Configuration configuration) {
		final YarnConfigOptions.UserJarInclusion userJarInclusion = configuration
				.getEnum(YarnConfigOptions.UserJarInclusion.class, YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
		final Optional<File> userLibDir = tryFindUserLibDirectory();

		checkState(
				userJarInclusion != YarnConfigOptions.UserJarInclusion.DISABLED || userLibDir.isPresent(),
				"The %s is set to %s. But the usrlib directory does not exist.",
				YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR.key(),
				YarnConfigOptions.UserJarInclusion.DISABLED);

		return userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? userLibDir.get() : null;
	}

	public static void main(String[] args) throws ProgramInvocationException {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnApplicationClusterEntryPoint.class.getSimpleName(), args);
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
		overwriteDetachedModeAndExecutor(configuration);
		updateConfigWithInterpretedJarURLs(configuration);

		final YarnApplicationClusterEntryPoint yarnApplicationClusterEntrypoint =
				new YarnApplicationClusterEntryPoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(yarnApplicationClusterEntrypoint);
	}

	private static void overwriteDetachedModeAndExecutor(final Configuration configuration) {
		requireNonNull(configuration);
		configuration.set(DeploymentOptions.ATTACHED, true);
		configuration.setString(ClusterEntrypoint.EXECUTION_MODE, ExecutionMode.NORMAL.toString());
		configuration.set(DeploymentOptions.TARGET, EmbeddedApplicationExecutor.NAME);
	}

	private static void updateConfigWithInterpretedJarURLs(final Configuration configuration) throws ProgramInvocationException {
		requireNonNull(configuration);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, interpretJarURLs(configuration), URL::toString);
	}

	private static List<URL> interpretJarURLs(final Configuration configuration) throws ProgramInvocationException {
		requireNonNull(configuration);
		return ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, path -> {
			try {
				return new File(path).getAbsoluteFile().toURI().toURL();
			} catch (MalformedURLException e) {
				throw new ProgramInvocationException(e);
			}
		});
	}
}
