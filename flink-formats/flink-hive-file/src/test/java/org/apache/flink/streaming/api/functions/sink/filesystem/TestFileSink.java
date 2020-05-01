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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

/**
 *
 */
public class TestFileSink<IN>
	extends RichSinkFunction<IN>
	implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {

	// -------------------------- state descriptors ---------------------------

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
		new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
		new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	// ------------------------ configuration fields --------------------------

	private final long bucketCheckInterval;

	private final HadoopPathBasedBulkFormatBuilder<IN, ?, ? extends HadoopPathBasedBulkFormatBuilder<IN, ?, ?>> bucketsBuilder;

	// --------------------------- runtime fields -----------------------------

	private transient Buckets<IN, ?> buckets;

	private transient ProcessingTimeService processingTimeService;

	// --------------------------- State Related Fields -----------------------------

	private transient ListState<byte[]> bucketStates;

	private transient ListState<Long> maxPartCountersState;

	public TestFileSink(
		HadoopPathBasedBulkFormatBuilder<IN, ?, ? extends HadoopPathBasedBulkFormatBuilder<IN, ?, ?>> bucketsBuilder,
		long bucketCheckInterval) {

		Preconditions.checkArgument(bucketCheckInterval > 0L);

		this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
		this.bucketCheckInterval = bucketCheckInterval;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		this.buckets = bucketsBuilder.createBuckets(subtaskIndex);

		final OperatorStateStore stateStore = context.getOperatorStateStore();
		bucketStates = stateStore.getListState(BUCKET_STATE_DESC);
		maxPartCountersState = stateStore.getUnionListState(MAX_PART_COUNTER_STATE_DESC);

		if (context.isRestored()) {
			buckets.initializeState(bucketStates, maxPartCountersState);
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		buckets.commitUpToCheckpoint(checkpointId);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(bucketStates != null && maxPartCountersState != null, "sink has not been initialized");

		buckets.snapshotState(
			context.getCheckpointId(),
			bucketStates,
			maxPartCountersState);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
		processingTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		final long currentTime = processingTimeService.getCurrentProcessingTime();
		buckets.onProcessingTime(currentTime);
		processingTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		buckets.onElement(value, context);
	}

	@Override
	public void close() throws Exception {
		if (buckets != null) {
			buckets.close();
		}
	}
}
