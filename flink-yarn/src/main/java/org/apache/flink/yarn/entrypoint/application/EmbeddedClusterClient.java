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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An embedded {@link ClusterClient} that uses directly the cluster's {@link DispatcherGateway}.
 * This is meant to be used when executing a job in {@code Application Mode}.
 */
@Internal
public class EmbeddedClusterClient implements ClusterClient<String> {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedClusterClient.class);

	private final Configuration configuration;

	private final DispatcherGateway dispatcherGateway;

	private final Time timeout;

	public EmbeddedClusterClient(
			final Configuration configuration,
			final DispatcherGateway dispatcherGateway) {
		this.configuration = checkNotNull(configuration);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
		this.timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	public String getClusterId() {
		return EmbeddedApplicationExecutor.NAME;
	}

	@Override
	public Configuration getFlinkConfiguration() {
		return configuration;
	}

	@Override
	public void shutDownCluster() {
		dispatcherGateway
				.shutDownCluster()
				.thenRun(() -> LOG.info("Cluster was shutdown."));
	}

	@Override
	public String getWebInterfaceURL() {
		throw new UnsupportedOperationException("The embedded client is not expected to know about a web URL.");
	}

	@Override
	public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
		return dispatcherGateway.requestMultipleJobDetails(timeout)
				.thenApply(jobDetails ->
						jobDetails.getJobs().stream().map(job ->
								new JobStatusMessage(
										job.getJobId(),
										job.getJobName(),
										job.getStatus(),
										job.getStartTime()
								)).collect(Collectors.toList())
				);
	}

	@Override
	public CompletableFuture<Acknowledge> disposeSavepoint(final String savepointPath) {
		checkNotNull(savepointPath);
		return dispatcherGateway.disposeSavepoint(savepointPath, timeout);
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
						})
				.thenApply(ack -> jobGraph.getJobID());
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus(final JobID jobId) {
		checkNotNull(jobId);
		return dispatcherGateway.requestJobStatus(jobId, timeout);
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(final JobID jobId) {
		checkNotNull(jobId);
		return dispatcherGateway.requestJobResult(jobId, RpcUtils.INF_TIMEOUT);
	}

	@Override
	public CompletableFuture<Map<String, Object>> getAccumulators(final JobID jobId, final ClassLoader loader) {
		checkNotNull(jobId);
		checkNotNull(loader);
		return dispatcherGateway.requestJob(jobId, timeout)
				.thenApply(ArchivedExecutionGraph::getAccumulatorsSerialized)
				.thenApply(accumulators -> {
					try {
						return AccumulatorHelper.deserializeAndUnwrapAccumulators(accumulators, loader);
					} catch (Exception e) {
						throw new CompletionException("Cannot deserialize and unwrap accumulators properly.", e);
					}
				});
	}

	@Override
	public CompletableFuture<Acknowledge> cancel(final JobID jobId) {
		checkNotNull(jobId);
		return dispatcherGateway.cancelJob(jobId, timeout);
	}

	@Override
	public CompletableFuture<String> cancelWithSavepoint(final JobID jobId, @Nullable String savepointDirectory) {
		checkNotNull(jobId);
		return dispatcherGateway.triggerSavepoint(jobId, savepointDirectory, true, timeout);
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(final JobID jobId, final boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
		checkNotNull(jobId);
		return dispatcherGateway.stopWithSavepoint(jobId, savepointDirectory, advanceToEndOfEventTime, timeout);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(final JobID jobId, @Nullable String savepointDirectory) {
		checkNotNull(jobId);
		return dispatcherGateway.triggerSavepoint(jobId, savepointDirectory, false, timeout);
	}
}
