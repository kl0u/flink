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

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class CommitterStateSerializer<S> implements SimpleVersionedSerializer<CommitterState<S>> {

	private static final int MAGIC_NUMBER = 0x1e764b79;

	private final SimpleVersionedSerializer<S> committableSerializer;

	public CommitterStateSerializer(final SimpleVersionedSerializer<S> committableSerializer) {
		this.committableSerializer = checkNotNull(committableSerializer);
	}

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(final CommitterState<S> state) throws IOException {
		checkNotNull(state);

		DataOutputSerializer out = new DataOutputSerializer(256);
		out.writeInt(MAGIC_NUMBER);
		serializeV0(state, out);
		return out.getCopyOfBuffer();
	}

	private void serializeV0(final CommitterState<S> state, final DataOutputView dataOutputView) throws IOException {
		final Set<Map.Entry<Long, List<S>>> entries = state.entrySet();

		dataOutputView.writeInt(entries.size());
		for (Map.Entry<Long, List<S>> committablesForCheckpoint : entries) {
			final List<S> committables = committablesForCheckpoint.getValue();

			dataOutputView.writeLong(committablesForCheckpoint.getKey());
			dataOutputView.writeInt(committables.size());

			for (S committable : committables) {
				SimpleVersionedSerialization
						.writeVersionAndSerialize(committableSerializer, committable, dataOutputView);
			}
		}
	}

	@Override
	public CommitterState<S> deserialize(int version, byte[] serialized) throws IOException {
		checkNotNull(serialized);
		final DataInputDeserializer in = new DataInputDeserializer(serialized);

		switch (version) {
			case 0:
				validateMagicNumber(in);
				return deserializeV0(in);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	private CommitterState<S> deserializeV0(final DataInputDeserializer in) throws IOException {
		final CommitterState<S> committerState = new CommitterState<>();
		final int size = in.readInt();
		for (int i = 0; i < size; i++) {
			final long checkpointId = in.readLong();
			final int noOfCommittables = in.readInt();

			final List<S> committables = new ArrayList<>(noOfCommittables);
			for (int j = 0; j < noOfCommittables; j++) {
				final S committable = SimpleVersionedSerialization
						.readVersionAndDeSerialize(committableSerializer, in);
				committables.add(committable);
			}
			committerState.put(checkpointId, committables);
		}
		return committerState;
	}

	private static void validateMagicNumber(DataInputView in) throws IOException {
		final int magicNumber = in.readInt();
		if (magicNumber != MAGIC_NUMBER) {
			throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
		}
	}
}
