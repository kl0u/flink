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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Javadoc. todo the remaining bit
 */
public class SinkCoordinator<Committable> implements OperatorCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(SinkCoordinator.class);

	private final Sink<?, Committable, ?> sink;

	private final String operatorName;

	private transient Committer<Committable> committer;
	private transient CommitterState<Committable> state;
	private transient SimpleVersionedSerializer<Committable> serializer;

	private transient CommitterStateSerializer<Committable> stateSerializer;

	private transient Map<Integer, List<Committable>> committablesPerSubtask;

	private transient boolean started;

	private final Context context;

	public SinkCoordinator(
			final String sinkName,
			final Sink<?, Committable, ?> sink,
			final Context context) {
		this.operatorName = checkNotNull(sinkName);
		this.sink = checkNotNull(sink);
		this.context = checkNotNull(context);
	}

	@Override
	public void start() throws Exception {
		this.committer = sink.createCommitter();
		this.state = new CommitterState<>();
		this.serializer = sink.getCommittableSerializer();
		this.stateSerializer = new CommitterStateSerializer<>(serializer);
		this.committablesPerSubtask = new HashMap<>();
		this.started = true;

		LOG.info("Started the Sink Coordinator of the {}", operatorName);
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) {
		ensureStarted();

		LOG.debug("Sink Coordinator {} received from subtask {} event: {} .", operatorName, subtask, event);
		try {
			if (event instanceof SinkOperatorEvent) {
				final SinkOperatorEvent<Committable> sinkEvent = (SinkOperatorEvent<Committable>) event;

				final List<Committable> committables = committablesPerSubtask
						.computeIfAbsent(subtask, k -> new ArrayList<>());
				committables.addAll(sinkEvent.getCommittables(serializer));
			} else {
				throw new IllegalStateException("Unknown message " + event.getClass().getCanonicalName());
			}
		} catch (IOException e) {
			context.failJob(e);
		}
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		ensureStarted();

		try {
			LOG.debug("Handling failure of subtask " + subtask + ".");
			for (Committable committable : committablesPerSubtask.remove(subtask)) {
				committer.abort(committable);
			}
		} catch (Exception e) {
			LOG.error("Aborting pending transactions for subtask {} failed due to {}", subtask, e);
			context.failJob(e);
		}
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		ensureStarted();

		this.committablesPerSubtask = new HashMap<>();
		this.state = SimpleVersionedSerialization
				.readVersionAndDeSerialize(stateSerializer, checkpointData);
	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
		ensureStarted();

		try {
			LOG.debug("Taking a state snapshot on the coordinator of {} for checkpoint {}.", operatorName, checkpointId);

			final List<Committable> committables = committablesPerSubtask
					.values()
					.stream()
					.flatMap(List::stream)
					.collect(Collectors.toList());
			this.state.put(checkpointId, committables);

			resultFuture.complete(
					SimpleVersionedSerialization.writeVersionAndSerialize(stateSerializer, state));

			this.committablesPerSubtask = new HashMap<>();
		} catch (Exception e) {
			resultFuture.completeExceptionally(new CompletionException(e));
		}
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		// do nothing as we only commit on successful completion of the application.
	}

	private void commitAll() {
		ensureStarted();

		LOG.debug("Committing pending transactions...");
		try {
			this.state.consumeUpTo(Long.MAX_VALUE, commitable -> committer.commit(commitable));
		} catch (Exception e) {
			LOG.error("Coordinator of {} failed to commit pending transactions on close() due to {}.", operatorName, e);
			context.failJob(e);
		}

		if (!committablesPerSubtask.isEmpty()) {
			LOG.error("Unstaged committables were expected to be empty but they are not.");
			// TODO: 20.08.20 we should probably fail the job here.
		}
	}

	@Override
	public void close() throws Exception {
		ensureStarted();
		// TODO: 15.08.20 is it safe to commit here??? The javadoc is a bit unclear. Is this called also on failure???

		this.started = false;
	}

	private void ensureStarted() {
		checkState(started, "The sink coordinator for " + operatorName + " has not been started yet.");
	}
}
