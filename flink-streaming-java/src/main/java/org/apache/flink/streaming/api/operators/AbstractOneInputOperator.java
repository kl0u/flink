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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.util.Preconditions;

/**
 * Javadoc.
 */
public abstract class AbstractOneInputOperator<I, O, F extends Function> {

	private final AbstractStreamOperator<O> operator;

	private final F function;

	private Output<StreamRecord<O>> output;

	private TimerService timerService;

	private TimestampedCollector<O> collector;

	private long watermark;

	AbstractOneInputOperator(
			final AbstractStreamOperator<O> operator,
			final F function
	) {
		this.function = Preconditions.checkNotNull(function);
		this.operator = Preconditions.checkNotNull(operator);
		this.output = this.operator.getOutput();

		this.watermark = Long.MIN_VALUE;
	}

	protected long getCurrentWatermark() {
		return watermark;
	}

	protected AbstractStreamOperator<O> getOperator() {
		return operator;
	}

	protected F getFunction() {
		return function;
	}

	protected Output<StreamRecord<O>> getOutput() {
		return output;
	}

	protected TimestampedCollector<O> getCollector() {
		return collector;
	}

	protected TimerService getTimerService() {
		return timerService;
	}

	// ------------------------------------------------------------------------
	//  operator life cycle
	// ------------------------------------------------------------------------

	public void setup(final StreamingRuntimeContext context) {
		FunctionUtils.setFunctionRuntimeContext(function, context);
	}

	// TODO: 9/16/18 the argument here should be an initialization context where we put all we want.
	public void open(final TimestampedCollector<O> collector, final TimerService timerService) throws Exception {
		FunctionUtils.openFunction(function, new Configuration());
		this.collector = collector;
		this.timerService = timerService;
	}

	public void initializeState(StateInitializationContext context) throws Exception {
		StreamingFunctionUtils.restoreFunctionState(context, function);
	}

	public void snapshotState(StateSnapshotContext context, OperatorStateBackend operatorStateBackend) throws Exception {
		StreamingFunctionUtils.snapshotFunctionState(context, operatorStateBackend, function);
	}

	public void close() throws Exception {
		FunctionUtils.closeFunction(function);
	}

	public void dispose() throws Exception {
		FunctionUtils.closeFunction(function);
	}

	// ------------------------------------------------------------------------
	//  element processing
	// ------------------------------------------------------------------------

	public abstract void processElement(StreamRecord<I> element) throws Exception;

	public void processWatermark(Watermark mark) throws Exception {
		this.watermark = mark.getTimestamp();
		operator.processWatermark(mark);
	}

	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		operator.processLatencyMarker(latencyMarker);
	}
}
