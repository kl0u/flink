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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.runner.DispatcherInitializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class ApplicationDispatcherInitializer implements DispatcherInitializer {

	private final Collection<JobGraph> recoveredJobs;

	private final ApplicationRunner applicationRunner;

	public ApplicationDispatcherInitializer(
			final Collection<JobGraph> recoveredJobGraphs,
			final ApplicationRunner applicationRunner) {
		this.recoveredJobs = requireNonNull(recoveredJobGraphs);
		this.applicationRunner = requireNonNull(applicationRunner);
	}

	@Override
	public Collection<JobGraph> getInitJobGraphs() {
		return recoveredJobs;
	}

	@Override
	public void bootstrap(final Dispatcher dispatcher) {
		requireNonNull(dispatcher);

		for (JobGraph recoveredJob : recoveredJobs) {
			FutureUtils.assertNoException(dispatcher.runJob(recoveredJob)
					.handle(dispatcher.handleRecoveredJobStartError(recoveredJob.getJobID())));
		}

		handleApplicationState(dispatcher);
	}

	private void handleApplicationState(final Dispatcher dispatcher) {
		requireNonNull(dispatcher);

		final List<JobID> recoveredJobIds = recoveredJobs
				.stream()
				.map(JobGraph::getJobID)
				.collect(Collectors.toList());

		final RpcService rpcService = dispatcher.getRpcService();
		CompletableFuture.runAsync(() -> {
			try {
				applicationRunner.run(recoveredJobIds, dispatcher);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}, rpcService.getExecutor());
	}
}
