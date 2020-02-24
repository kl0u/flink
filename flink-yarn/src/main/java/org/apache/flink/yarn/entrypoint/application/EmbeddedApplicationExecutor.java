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

package org.apache.flink.yarn.entrypoint.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.executors.ExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class EmbeddedApplicationExecutor implements PipelineExecutor {

	public static final String NAME = "Embedded";

	private final EmbeddedClusterClient clusterClient;

	public EmbeddedApplicationExecutor(
			final Configuration configuration,
			final DispatcherGateway dispatcherGateway) {

		this.clusterClient = new EmbeddedClusterClient(
				checkNotNull(configuration),
				checkNotNull(dispatcherGateway));
	}

	@Override
	public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) {
		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);

		return clusterClient
				.submitJob(jobGraph)
				.thenApplyAsync(jobID -> (JobClient) new ClusterClientJobClientAdapter<>(
						() -> clusterClient,
						jobID))
				.whenComplete((ignored1, ignored2) -> clusterClient.close());
	}
}
