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
import org.apache.flink.api.connector.sink.WriterOutput;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class CommittableHandler<Committable> implements WriterOutput<Committable> {

	// TODO: 17.08.20 this class has too many responsibilities. Break it.

	private final Committer<Committable> committer;

	private final CommitterState<Committable> committerState;

	private final SimpleVersionedSerializer<Committable> committableSerializer;

	private final CommitterStateSerializer<Committable> committerStateSerializer;

	private final OperatorEventGateway coordinator;

	private List<Committable> unstagedCommittables;

	public CommittableHandler(
			final Committer<Committable> committer,
			final SimpleVersionedSerializer<Committable> committableSerializer,
			final OperatorEventGateway sinkCoordinator,
			final List<CommitterState<Committable>> initialStates) {
		this.committer = checkNotNull(committer);
		this.committableSerializer = checkNotNull(committableSerializer);
		this.committerStateSerializer = new CommitterStateSerializer<>(committableSerializer);

		this.coordinator = checkNotNull(sinkCoordinator);
		this.unstagedCommittables = new ArrayList<>();

		// todo this can become a method in the committerstate
		this.committerState = new CommitterState<>();
		for (CommitterState<Committable> state : checkNotNull(initialStates)) {
			committerState.merge(state);
		}
	}

	byte[] snapshotState(
			final long checkpointId) throws Exception {
		this.committerState.put(checkpointId, unstagedCommittables);
		this.unstagedCommittables = new ArrayList<>();
		return SimpleVersionedSerialization
				.writeVersionAndSerialize(committerStateSerializer, committerState);
	}

	void flush() throws Exception {
		this.committerState.put(Long.MAX_VALUE, unstagedCommittables);
		this.unstagedCommittables = new ArrayList<>();

		// TODO: 17.08.20 list or single event in the sink event?
		this.committerState.consumeUpTo(
				Long.MAX_VALUE,
				c -> coordinator.sendEventToCoordinator(
						new SinkOperatorEvent<>(
								Collections.singletonList(c),
								committableSerializer)));
	}

	void onCheckpointCompleted(final long checkpointId) throws Exception {
		committerState.consumeUpTo(checkpointId, committer::commit);
	}

	@Override
	public void sendToCommit(Committable committable) {
		if (committable == null) {
			return;
		}
		this.unstagedCommittables.add(committable);
	}
}
