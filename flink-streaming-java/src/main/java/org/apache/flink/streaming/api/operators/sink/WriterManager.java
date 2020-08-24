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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.connector.sink.WriterOutput;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateSnapshotContext;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc. todo Change name, clean it up and integrate it in the StreamOperator.
 */
public class WriterManager<IN, CommittableT, WriterStateT, CommonStateT> {

	private final ListState<byte[]> writerStateStore;
	private final ListState<byte[]> sharedWriterStateStore;

	private final SimpleVersionedSerializer<WriterStateT> writerStateSerializer;
	private final SimpleVersionedSerializer<CommonStateT> sharedWriterStateSerializer;

	private final List<WriterStateT> restoredWriterStates;
	private final List<CommonStateT> restoredSharedWriterStates;

	private final Writer<IN, CommittableT, WriterStateT, CommonStateT> writer;

	private WriterManager(
			final Writer<IN, CommittableT, WriterStateT, CommonStateT> writer,
			final ListState<byte[]> writerStateStore,
			final ListState<byte[]> sharedWriterStateStore,
			final SimpleVersionedSerializer<WriterStateT> writerStateSerializer,
			final SimpleVersionedSerializer<CommonStateT> sharedWriterStateSerializer,
			final List<WriterStateT> restoredWriterStates,
			final List<CommonStateT> restoredSharedWriterStates) {
		this.writer = checkNotNull(writer);
		this.writerStateStore = checkNotNull(writerStateStore);
		this.sharedWriterStateStore = checkNotNull(sharedWriterStateStore);
		this.writerStateSerializer = checkNotNull(writerStateSerializer);
		this.sharedWriterStateSerializer = checkNotNull(sharedWriterStateSerializer);
		this.restoredWriterStates = checkNotNull(restoredWriterStates);
		this.restoredSharedWriterStates = checkNotNull(restoredSharedWriterStates);
	}

	void write(IN element, Writer.Context<CommittableT> ctx, WriterOutput<CommittableT> output) throws Exception {
		writer.write(element, ctx, output);
	}

	void snapshotState(StateSnapshotContext context, WriterOutput<CommittableT> writerOutput) throws Exception {
		cleanupStateStores();

		final long checkpointId = context.getCheckpointId();
		if (checkpointId == Long.MAX_VALUE) {
			// TODO: 17.08.20 this is the FINAL checkpoint before closing
			//  the flag should be set in the context like: StateSnapshotContext.isFinalCheckpoint())
			//  but in order to compile I leave it like this.
			// Before I was sending the last bit to the coordinator to commit it.
			writer.flush(writerOutput);
		}

		// in any case we snapshot the state of the writer because the committables may
		// have been shipped but the writer may have other state to checkpoint, e.g. in
		// case of SUSPEND

		snapshotCommonState();
		snapshotSubtaskState(writerOutput);
	}

	private void snapshotCommonState() throws Exception {
		final CommonStateT commonState = writer.snapshotSharedState();
		final byte[] serializedCommonState = SimpleVersionedSerialization
				.writeVersionAndSerialize(sharedWriterStateSerializer, commonState);
		sharedWriterStateStore.add(serializedCommonState);
	}

	private void snapshotSubtaskState(final WriterOutput<CommittableT> writerOutput) throws Exception {
		final List<WriterStateT> writerState = writer.snapshotState(writerOutput);
		for (WriterStateT state : writerState) {
			final byte[] serializedSubtaskState = SimpleVersionedSerialization
					.writeVersionAndSerialize(writerStateSerializer, state);
			writerStateStore.add(serializedSubtaskState);
		}
	}

	private void cleanupStateStores() {
		sharedWriterStateStore.clear();
		writerStateStore.clear();
	}

	private void cleanupRestoredStates() {
		restoredWriterStates.clear();
		restoredSharedWriterStates.clear();
	}

	static <IN, CommittableT, WriterStateT, CommonStateT> WriterManager<IN, CommittableT, WriterStateT, CommonStateT> create(
			final Writer<IN, CommittableT, WriterStateT, CommonStateT> writer,
			final SimpleVersionedSerializer<WriterStateT> writerStateSerializer,
			final SimpleVersionedSerializer<CommonStateT> sharedWriterStateSerializer,
			final ListState<byte[]> writerStateStore,
			final ListState<byte[]> sharedWriterStateStore) {
		return new WriterManager<>(
				writer,
				writerStateStore,
				sharedWriterStateStore,
				writerStateSerializer,
				sharedWriterStateSerializer,
				new ArrayList<>(),
				new ArrayList<>());
	}

	static <IN, CommittableT, WriterStateT, CommonStateT> WriterManager<IN, CommittableT, WriterStateT, CommonStateT> restore(
			final Writer<IN, CommittableT, WriterStateT, CommonStateT> writer,
			final WriterOutput<CommittableT> output,
			final SimpleVersionedSerializer<WriterStateT> writerStateSerializer,
			final SimpleVersionedSerializer<CommonStateT> sharedWriterStateSerializer,
			final ListState<byte[]> writerStateStore,
			final ListState<byte[]> sharedWriterStateStore) throws Exception {

		final List<WriterStateT> restoredWriterStates =
				deserializeStates(writerStateStore, writerStateSerializer);
		final List<CommonStateT> restoredSharedWriterStates =
				deserializeStates(sharedWriterStateStore, sharedWriterStateSerializer);
		writer.init(restoredWriterStates, restoredSharedWriterStates, output);
		return new WriterManager<>(
				writer,
				writerStateStore,
				sharedWriterStateStore,
				writerStateSerializer,
				sharedWriterStateSerializer,
				restoredWriterStates,
				restoredSharedWriterStates);
	}

	private static <T> List<T> deserializeStates(
			final ListState<byte[]> serializedStates,
			final SimpleVersionedSerializer<T> serializer) throws Exception {
		final List<T> result = new ArrayList<>();
		for (byte[] state : serializedStates.get()) {
			final T deserializedState = SimpleVersionedSerialization
					.readVersionAndDeSerialize(serializer, state);
			result.add(deserializedState);
		}
		return result;
	}
}
