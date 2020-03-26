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

package org.apache.flink.client.deployment.application.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.application.EmbeddedClient;
import org.apache.flink.client.deployment.executors.ExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * A {@link PipelineExecutor} that is expected to be executed on the same machine as the
 * {@link org.apache.flink.runtime.dispatcher.DispatcherGateway Dispatcher} and uses it directly
 * to submit and monitor jobs.
 */
@Internal
public class EmbeddedExecutor implements PipelineExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedExecutor.class);

	public static final String NAME = "embedded";

	private final Collection<JobID> applicationJobIds;

	private final DispatcherGateway dispatcherGateway;

	public EmbeddedExecutor(
			final Collection<JobID> applicationJobIds,
			final DispatcherGateway gateway) {
		this.applicationJobIds = requireNonNull(applicationJobIds);
		this.dispatcherGateway = requireNonNull(gateway);
	}

	@Override
	public CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) {
		requireNonNull(pipeline);
		requireNonNull(configuration);

		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);
		final JobID actualJobId = jobGraph.getJobID();

		this.applicationJobIds.add(actualJobId);
		LOG.info("Job {} is submitted.", actualJobId);

		final EmbeddedClient embeddedClient = new EmbeddedClient(actualJobId, configuration, dispatcherGateway);
		return embeddedClient
				.submitJob(jobGraph)
				.thenApplyAsync(jobID -> embeddedClient);
	}
}
