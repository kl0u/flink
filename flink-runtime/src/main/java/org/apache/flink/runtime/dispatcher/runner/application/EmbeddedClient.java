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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An embedded {@link JobClient} with the ability to submit jobs.
 * This client is meant to be used when executing a job in {@code Application Mode}, where
 * the user's main is executed on the cluster, and uses directly the cluster's {@link DispatcherGateway}.
 */
@Internal
public class EmbeddedClient implements JobClient, JobGraphDeployer {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedClient.class);

	private final JobID jobId;

	private final Configuration configuration;

	private final DispatcherGateway dispatcherGateway;

	private final Time timeout;

	public EmbeddedClient(
			final JobID jobId,
			final Configuration configuration,
			final DispatcherGateway dispatcherGateway) {
		this.jobId = checkNotNull(jobId);
		this.configuration = checkNotNull(configuration);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
		this.timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));
	}

	@Override
	public CompletableFuture<JobID> submitJob(final JobGraph jobGraph) {
		checkNotNull(jobGraph);

		LOG.info("Submitting Job with JobId={}.", jobGraph.getJobID());

		return dispatcherGateway
				.getBlobServerPort(timeout)
				.thenApply(blobServerPort -> new InetSocketAddress(dispatcherGateway.getHostname(), blobServerPort))
				.thenCompose(blobServerAddress -> {

					try {
						ClientUtils.extractAndUploadJobGraphFiles(jobGraph, () -> new BlobClient(blobServerAddress, configuration));
					} catch (FlinkException e) {
						throw new CompletionException(e);
					}

					return dispatcherGateway.submitJob(jobGraph, timeout);
				}).thenApply(ack -> jobGraph.getJobID());
	}

	@Override
	public JobID getJobID() {
		return jobId;
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus() {
		return dispatcherGateway.requestJobStatus(jobId, timeout);
	}

	@Override
	public CompletableFuture<Void> cancel() {
		return dispatcherGateway
				.cancelJob(jobId, timeout)
				.thenApply(ignores -> null);
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(final boolean advanceToEndOfEventTime, @Nullable final String savepointDirectory) {
		return dispatcherGateway.stopWithSavepoint(jobId, savepointDirectory, advanceToEndOfEventTime, timeout);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable final String savepointDirectory) {
		return dispatcherGateway.triggerSavepoint(jobId, savepointDirectory, false, timeout);
	}

	@Override
	public CompletableFuture<Map<String, Object>> getAccumulators(final ClassLoader classLoader) {
		checkNotNull(classLoader);

		return dispatcherGateway.requestJob(jobId, timeout)
				.thenApply(ArchivedExecutionGraph::getAccumulatorsSerialized)
				.thenApply(accumulators -> {
					try {
						return AccumulatorHelper.deserializeAndUnwrapAccumulators(accumulators, classLoader);
					} catch (Exception e) {
						throw new CompletionException("Cannot deserialize and unwrap accumulators properly.", e);
					}
				});
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(final ClassLoader userClassloader) {
		checkNotNull(userClassloader);

		return dispatcherGateway
				.requestJobResult(jobId, RpcUtils.INF_TIMEOUT)
				.thenApply((jobResult) -> {
					try {
						return jobResult.toJobExecutionResult(userClassloader);
					} catch (Throwable t) {
						throw new CompletionException(
								new Exception("Job " + jobId + " failed", t));
					}
				});
	}
}
