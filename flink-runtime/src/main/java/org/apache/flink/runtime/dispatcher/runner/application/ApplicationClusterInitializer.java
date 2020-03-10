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

package org.apache.flink.runtime.dispatcher.runner.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.runner.ClusterInitializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Javadoc.
 */
@Internal
public class ApplicationClusterInitializer implements ClusterInitializer {

	private final Collection<JobGraph> recoveredJobs;

	private final ApplicationHandler applicationSubmitter;

	public ApplicationClusterInitializer(
			final Collection<JobGraph> recoveredJobs,
			final ApplicationHandler applicationSubmitter) {
		this.recoveredJobs = requireNonNull(recoveredJobs);
		this.applicationSubmitter = requireNonNull(applicationSubmitter);
	}

	@Override
	public Collection<JobGraph> getInitJobGraphs() {
		return recoveredJobs;
	}

	@Override
	public void initializeCluster(final Dispatcher dispatcher) {
		requireNonNull(dispatcher);

		final JobID applicationIdToSubmit = applicationSubmitter.getJobId();

		boolean recoveredApplicationId = false;
		for (JobGraph recoveredJob : recoveredJobs) {
			if (recoveredJob.getJobID().equals(applicationIdToSubmit)) {
				recoveredApplicationId = true;
			}

			FutureUtils.assertNoException(dispatcher.runJob(recoveredJob)
					.handle(dispatcher.handleRecoveredJobStartError(recoveredJob.getJobID())));
		}

		recoveredJobs.clear();

		if (applicationIdToSubmit != null) {
			handleApplicationState(dispatcher, recoveredApplicationId);
		}
	}

	private void handleApplicationState(final Dispatcher dispatcher, final boolean onRecovery) {
		requireNonNull(dispatcher);

		final RpcService rpcService = dispatcher.getRpcService();

		CompletableFuture.runAsync(() -> {
			try {
				if (onRecovery) {
					applicationSubmitter.recover(dispatcher);
				} else {
					applicationSubmitter.submit(dispatcher);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}, rpcService.getExecutor());
	}
}
