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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Javadoc.
 * FOR NOW, I assume that this may be used also in streaming, that is why I have implemented
 * also the checkpoint-related methods. If not needed, we can remove them in the future.
 */
public class SinkCoordinator<Committable> implements OperatorCoordinator, JobStatusListener {

	private static final Logger LOG = LoggerFactory.getLogger(SinkCoordinator.class);

	private final String operatorName;

	private final Sink<?, Committable, ?, ?> sink;

	private final Context context;

	private Committer<Committable> committer;
	private CommitterState<Committable> state;
	private SimpleVersionedSerializer<Committable> serializer;
	private CommitterStateSerializer<Committable> stateSerializer;

	private Map<Integer, List<Committable>> unstagedCommittablesPerSubtask;

	private volatile boolean started;

	public SinkCoordinator(
			final String sinkName,
			final Sink<?, Committable, ?, ?> sink,
			final Context context) {
		this.operatorName = checkNotNull(sinkName);
		this.sink = checkNotNull(sink);
		this.context = checkNotNull(context);
	}

	@Override
	public void start() throws Exception {
		this.committer = sink.createCommitter();
		this.serializer = sink.getCommittableSerializer();
		this.stateSerializer = new CommitterStateSerializer<>(serializer);

		this.state = new CommitterState<>();
		this.unstagedCommittablesPerSubtask = new HashMap<>();
		this.started = true;

		LOG.info("Started the Sink Coordinator for {}.", operatorName);
	}

	@Override
	public void close() throws Exception {
		LOG.debug("Sink coordinator for {} terminated and all its resources were freed.", operatorName);
		this.started = false;
	}

	@Override
	public void jobStatusChanges(final JobID jobId, final JobStatus newJobStatus, final long timestamp, final Throwable error) {
		ensureStarted();

		if (newJobStatus == JobStatus.FINISHED) {
			LOG.info("Job {} finished SUCCESSFULLY. Committing all pending files...", jobId);
			commitAll();
			LOG.info("Sink coordinator for {} of job {} committed all pending committables successfully.", operatorName, jobId);
			started = false;
		}
	}

	private void commitAll() {
		ensureStarted();

		// if we are in a streaming context,
		// we have need to commit any accumulated state
		commitStateUpToCheckpoint(Long.MAX_VALUE);
		commitUnstagedOnFinished();
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) {
		ensureStarted();

		LOG.debug("Sink Coordinator for {} received from subtask {} event: {} .", operatorName, subtask, event);
		try {
			if (event instanceof SinkOperatorEvent) {
				final SinkOperatorEvent<Committable> sinkEvent =
						(SinkOperatorEvent<Committable>) event;

				final List<Committable> committables = unstagedCommittablesPerSubtask
						.computeIfAbsent(subtask, k -> new ArrayList<>());
				committables.addAll(sinkEvent.getCommittables(serializer));
			}
		} catch (IOException e) {
			context.failJob(e);
		}
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		ensureStarted();

		try {
			LOG.debug("Sink coordinator for {} handling failure of subtask {}.", operatorName, subtask);
			for (Committable committable : unstagedCommittablesPerSubtask.remove(subtask)) {
				committer.abort(committable);
			}
		} catch (Exception e) {
			LOG.error("Sink coordinator for {} failed to abort pending transactions for failed subtask {}", operatorName, subtask, e);
			context.failJob(e);
		}
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		ensureStarted();

		LOG.debug("Sink coordinator for {} resetting its state.", operatorName);

		this.state = SimpleVersionedSerialization
				.readVersionAndDeSerialize(stateSerializer, checkpointData);
		this.unstagedCommittablesPerSubtask = new HashMap<>();
	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
		ensureStarted();

		try {
			LOG.debug("Sink coordinator for {} taking state snapshot for checkpoint {}.", operatorName, checkpointId);

			this.state.put(checkpointId, flatten(unstagedCommittablesPerSubtask.values()));
			this.unstagedCommittablesPerSubtask = new HashMap<>();

			resultFuture.complete(
					SimpleVersionedSerialization.writeVersionAndSerialize(stateSerializer, state));
		} catch (Exception e) {
			resultFuture.completeExceptionally(new CompletionException(e));
		}
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		ensureStarted();
		commitStateUpToCheckpoint(checkpointId);
	}

	private void commitStateUpToCheckpoint(final long checkpointId) {
		try {
			LOG.debug("Sink coordinator for {} committing up to checkpoint {}", operatorName, checkpointId);
			state.consumeUpTo(checkpointId, committer::commit);
		} catch (Exception e) {
			LOG.error("Sink coordinator for {} failing job because it failed to commit " +
					"state on checkpoint {}'s completion.", operatorName, checkpointId, e);
			context.failJob(e);
		}
	}

	private void commitUnstagedOnFinished() {
		try {
			for (Committable committable : flatten(unstagedCommittablesPerSubtask.values())) {
				committer.commit(committable);
			}
		} catch (Exception e) {
			LOG.error("Sink coordinator for {} failing job because it failed to commit " +
					" state at the end of the job.", operatorName, e);
			context.failJob(e);
		}
	}

	private void ensureStarted() {
		checkState(started, "The sink coordinator for " + operatorName + " has not been started yet.");
	}

	private static <T> List<T> flatten(final Collection<List<T>> nestedCollections) {
		return checkNotNull(nestedCollections)
				.stream()
				.flatMap(Collection::stream)
				.collect(Collectors.toList());
	}
}
