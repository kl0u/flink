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

package org.apache.flink.client.deployment.application;

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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JobClient} with the ability to also submit jobs which
 * uses directly the {@link DispatcherGateway}.
 */
@Internal
public class EmbeddedClient implements JobClient {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedClient.class);

	private final ScheduledExecutor retryExecutor;

	private final JobID jobId;

	private final Configuration configuration;

	private final DispatcherGateway dispatcherGateway;

	private final Time timeout;

	public EmbeddedClient(
			final JobID jobId,
			final Configuration configuration,
			final DispatcherGateway dispatcherGateway,
			final ScheduledExecutor retryExecutor) {
		this.retryExecutor = checkNotNull(retryExecutor);
		this.jobId = checkNotNull(jobId);
		this.configuration = checkNotNull(configuration);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
		this.timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));
	}

	// TODO: 02.04.20 check if it is needed to ship jars in the standalone/Kubernetes
	// TODO: 02.04.20 see if the jobgraph has any jars attached to it.
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

		return pollJobResultAsync(
				() -> dispatcherGateway.requestJobStatus(jobId, timeout),
				() -> dispatcherGateway.requestJobResult(jobId, timeout),
				100L
		).thenApply((jobResult) -> {
			try {
				return jobResult.toJobExecutionResult(userClassloader);
			} catch (Throwable t) {
				throw new CompletionException(new Exception("Job " + jobId + " failed", t));
			}
		});
	}

	private CompletableFuture<JobResult> pollJobResultAsync(
			final Supplier<CompletableFuture<JobStatus>> jobStatusSupplier,
			final Supplier<CompletableFuture<JobResult>> jobResultSupplier,
			final long retryMsTimeout) {
		return pollJobResultAsync(jobStatusSupplier, jobResultSupplier, new CompletableFuture<>(), retryMsTimeout, 0);
	}

	private CompletableFuture<JobResult> pollJobResultAsync(
			final Supplier<CompletableFuture<JobStatus>> jobStatusSupplier,
			final Supplier<CompletableFuture<JobResult>> jobResultSupplier,
			final CompletableFuture<JobResult> resultFuture,
			final long retryMsTimeout,
			final long attempt) {

		jobStatusSupplier.get().whenComplete((jobStatus, throwable) -> {
			if (throwable != null) {
				resultFuture.completeExceptionally(throwable);
			} else {
				if (jobStatus.isGloballyTerminalState()) {
					jobResultSupplier.get().whenComplete((jobResult, t) -> {

						if (LOG.isDebugEnabled()) {
							LOG.debug("Retrieved Job Result for Job {} after {} attempts.", jobId, attempt);
						}

						if  (t != null) {
							resultFuture.completeExceptionally(t);
						} else {
							resultFuture.complete(jobResult);
						}
					});
				} else {
					retryExecutor.schedule(() -> {
						pollJobResultAsync(jobStatusSupplier, jobResultSupplier, resultFuture, retryMsTimeout, attempt + 1);
					}, retryMsTimeout, TimeUnit.MILLISECONDS);
				}
			}
		});

		return resultFuture;
	}
}
