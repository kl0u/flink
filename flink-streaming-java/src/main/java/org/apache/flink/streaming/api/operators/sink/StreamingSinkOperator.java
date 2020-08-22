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
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.InitContext;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class StreamingSinkOperator<IN, Committable, WriterStateT, CommonStateT>
		extends AbstractStreamOperator<Object>
		implements OneInputStreamOperator<IN, Object> {

	private final Sink<IN, Committable, WriterStateT, CommonStateT> sink;

	// ------------------------- Runtime fields -------------------------

	private Writer<IN, Committable, WriterStateT, CommonStateT> writer;

	private WriterContext<Committable> writerContext;

	private CommittableHandler<Committable> committableHandler;

	private long lastWatermark;

	// ------------------------- State-related fields -------------------------

	private static final ListStateDescriptor<byte[]> WRITER_STATE_DESC =
			new ListStateDescriptor<>("writer-state", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<byte[]> SHARED_STATE_DESC =
			new ListStateDescriptor<>("shared-state", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<byte[]> COMMITTER_STATE_DESC =
			new ListStateDescriptor<>("committer-state", BytePrimitiveArraySerializer.INSTANCE);

	private ListState<byte[]> committerStateStore;
	private ListState<byte[]> sharedWriterStateStore;
	private ListState<byte[]> writerStateStore;

	private SimpleVersionedSerializer<WriterStateT> writerStateSerializer;
	private SimpleVersionedSerializer<CommonStateT> sharedWriterStateSerializer;

	private List<WriterStateT> restoredWriterStates;
	private List<CommonStateT> restoredSharedWriterStates;
	private List<CommitterState<Committable>> restoredCommitterStates;

	public StreamingSinkOperator(
			final Sink<IN, Committable, WriterStateT, CommonStateT> sink,
			final ProcessingTimeService timeService) {
		this.sink = checkNotNull(sink);
		this.processingTimeService = checkNotNull(timeService);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		writerStateStore = context.getOperatorStateStore().getListState(WRITER_STATE_DESC);
		sharedWriterStateStore = context.getOperatorStateStore().getUnionListState(SHARED_STATE_DESC);
		committerStateStore = context.getOperatorStateStore().getListState(COMMITTER_STATE_DESC);

		writerStateSerializer = sink.getStateSerializer();
		sharedWriterStateSerializer = sink.getSharedStateSerializer();

		restoredWriterStates = new ArrayList<>();
		restoredSharedWriterStates = new ArrayList<>();
		restoredCommitterStates = new ArrayList<>();

		if (context.isRestored()) {
			final CommitterStateSerializer<Committable> committerStateSerializer =
					new CommitterStateSerializer<>(sink.getCommittableSerializer());

			deserializeStates(writerStateStore, writerStateSerializer, restoredWriterStates);
			deserializeStates(sharedWriterStateStore, sharedWriterStateSerializer, restoredSharedWriterStates);
			deserializeStates(committerStateStore, committerStateSerializer, restoredCommitterStates);
		}

		lastWatermark = Long.MIN_VALUE;
	}

	private static <T> void deserializeStates(
			final ListState<byte[]> serializedStates,
			final SimpleVersionedSerializer<T> serializer,
			final List<T> result) throws Exception {
		for (byte[] state : serializedStates.get()) {
			final T deserializedState = SimpleVersionedSerialization
					.readVersionAndDeSerialize(serializer, state);
			result.add(deserializedState);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.committableHandler = new CommittableHandler<>(
				sink.createCommitter(),
				sink.getCommittableSerializer(),
				restoredCommitterStates);

		this.writer = sink.createWriter(createInitContext());
		this.writer.init(restoredWriterStates, restoredSharedWriterStates, committableHandler);

		cleanupRestoredStates();

		this.writerContext = new WriterContext<>(
				processingTimeService,
				committableHandler);
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		cleanupStateStores();

		snapshotCommonState();

		final long checkpointId = context.getCheckpointId();
		if (checkpointId == Long.MAX_VALUE) {
			// TODO: 17.08.20 this is the FINAL checkpoint before closing
			//  the flag should be set in the context like: context.isFinalCheckpoint())
			//  but in order to compile I leave it like this.
			// Before I was sending the last bit to the coordinator to commit it.
			writer.flush(committableHandler);
		} else {
			snapshotSubtaskState();
		}

		final Optional<byte[]> committables =
				committableHandler.snapshotState(checkpointId);
		if (committables.isPresent()) {
			committerStateStore.add(committables.get());
		}
	}

	private void snapshotCommonState() throws Exception {
		final CommonStateT commonState = writer.snapshotSharedState();
		final byte[] serializedCommonState = SimpleVersionedSerialization
				.writeVersionAndSerialize(sharedWriterStateSerializer, commonState);
		sharedWriterStateStore.add(serializedCommonState);
	}

	private void snapshotSubtaskState() throws Exception {
		final List<WriterStateT> writerState = writer.snapshotState(committableHandler);
		for (WriterStateT state : writerState) {
			final byte[] serializedSubtaskState = SimpleVersionedSerialization
					.writeVersionAndSerialize(writerStateSerializer, state);
			writerStateStore.add(serializedSubtaskState);
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		lastWatermark = mark.getTimestamp();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		committableHandler.onCheckpointCompleted(checkpointId);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		writerContext.update(element.getTimestamp(), lastWatermark);
		writer.write(element.getValue(), writerContext, committableHandler);
	}

	private void cleanupStateStores() {
		sharedWriterStateStore.clear();
		writerStateStore.clear();
		committerStateStore.clear();
	}

	private void cleanupRestoredStates() {
		restoredWriterStates.clear();
		restoredSharedWriterStates.clear();
		restoredCommitterStates.clear();
	}

	private InitContext createInitContext() {
		return new InitContext() {
			@Override
			public int getSubtaskId() {
				return getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public int getAttemptID() {
				return getRuntimeContext().getAttemptNumber();
			}

			@Override
			public MetricGroup metricGroup() {
				return getMetricGroup();
			}
		};
	}
}
