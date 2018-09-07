/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.NonKeyedSideInputProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Javadoc.
 */
public class NonKeyedMultiInputStreamOperator<O>
		extends MultiInputStreamOperator<O> {

	private static final long serialVersionUID = 1L;

	// TODO: 9/10/18 add logging
	private static final Logger LOG = LoggerFactory.getLogger(NonKeyedMultiInputStreamOperator.class);

	private final Map<InputTag, NonKeyedSideInputProcessFunction<?, O>> nonKeyedInputFunctions;

	private transient TimestampedCollector<O> collector;

	private transient List<CheckpointListener> checkpointListeners;

	/**
	 * Flag to prevent duplicate function.close() calls in close() and dispose().
	 */
	private boolean functionsClosed;

	// ---------------- runtime fields ------------------

	private transient Map<InputTag, NonKeyedOneInputOperator<?, O>> sideInputOperators;

	public NonKeyedMultiInputStreamOperator(final Map<InputTag, NonKeyedSideInputProcessFunction<?, O>> nonKeyedInputs) {
		this.nonKeyedInputFunctions = Preconditions.checkNotNull(nonKeyedInputs);
		Preconditions.checkArgument(!nonKeyedInputFunctions.isEmpty());
	}

	// ------------------------------------------------------------------------
	//  Output type configuration
	// ------------------------------------------------------------------------

	@Override
	public void setOutputType(TypeInformation<O> outTypeInfo, ExecutionConfig executionConfig) {
		// get a random function and get the output type from it,
		// as they are all expected to have the same output type.
		final Function function = nonKeyedInputFunctions.values().iterator().next();
		StreamingFunctionUtils.setOutputType(function, outTypeInfo, executionConfig);
	}

	// ------------------------------------------------------------------------
	//  operator life cycle
	// ------------------------------------------------------------------------

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<O>> output) {
		super.setup(containingTask, config, output);

		this.checkpointListeners = cacheCheckpointListeners();

		this.sideInputOperators = new HashMap<>(nonKeyedInputFunctions.size());
		for (Map.Entry<InputTag, NonKeyedSideInputProcessFunction<?, O>> e : nonKeyedInputFunctions.entrySet()) {
			final NonKeyedOneInputOperator<?, O> operator = new NonKeyedOneInputOperator<>(this, e.getValue());
			sideInputOperators.put(e.getKey(), operator);
			operator.setup(getRuntimeContext());
		}
		this.functionsClosed = false;
	}

	private List<CheckpointListener> cacheCheckpointListeners() {
		final List<CheckpointListener> listeners = new ArrayList<>();
		for (Function udf : nonKeyedInputFunctions.values()) {
			if (udf instanceof CheckpointListener) {
				listeners.add((CheckpointListener) udf);
			}
		}
		return listeners;
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		for (Map.Entry<InputTag, NonKeyedOneInputOperator<?, O>> e : sideInputOperators.entrySet()) {
			e.getValue().snapshotState(context, getOperatorStateBackend());
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		for (Map.Entry<InputTag, NonKeyedOneInputOperator<?, O>> e : sideInputOperators.entrySet()) {
			e.getValue().initializeState(context);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		collector = new TimestampedCollector<>(output);

		for (Map.Entry<InputTag, NonKeyedOneInputOperator<?, O>> e : sideInputOperators.entrySet()) {
			e.getValue().open(collector, null);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		for (Map.Entry<InputTag, NonKeyedOneInputOperator<?, O>> e : sideInputOperators.entrySet()) {
			e.getValue().close();
		}
		functionsClosed = true;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (!functionsClosed) {
			for (Map.Entry<InputTag, NonKeyedOneInputOperator<?, O>> e : sideInputOperators.entrySet()) {
				e.getValue().dispose();
			}
			functionsClosed = true;
		}
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		for (CheckpointListener listener : checkpointListeners) {
			listener.notifyCheckpointComplete(checkpointId);
		}
	}

	@Override
	public AbstractOneInputOperator<?, O, ?> getOperatorForInput(final InputTag tag) {
		final NonKeyedOneInputOperator<?, O> operator = sideInputOperators.get(tag);
		Preconditions.checkState(operator != null, "Unknown tag: " + tag);
		return operator;
	}
}
