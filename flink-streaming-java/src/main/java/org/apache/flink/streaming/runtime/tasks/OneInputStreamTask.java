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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.InputProcessorImpl;
import org.apache.flink.streaming.runtime.io.GeneralValveOutputHandler;
import org.apache.flink.streaming.runtime.io.InputProcessor;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link StreamTask} for executing a {@link OneInputStreamOperator}.
 */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private InputProcessor inputProcessor;

	private volatile boolean running = true;

	private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	public OneInputStreamTask(Environment env) {
		super(env);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link ProcessingTimeService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param env The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 */
	@VisibleForTesting
	public OneInputStreamTask(
			Environment env,
			@Nullable ProcessingTimeService timeProvider) {
		super(env, timeProvider);
	}

	@Override
	public void init() throws Exception {
		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs > 0) {
			InputGate[] inputGates = getEnvironment().getAllInputGates();

			List<Collection<InputGate>> inputGatesCol = new ArrayList<>();
			Collection<InputGate> inputGatesList = new ArrayList<>();
			Collections.addAll(inputGatesList, inputGates);
			inputGatesCol.add(inputGatesList);

			List<TypeSerializer<?>> inputSerializers = new ArrayList<>();
			inputSerializers.add(inSerializer);

			List<WatermarkGauge> inputWatermarkGauges = new ArrayList<>();
			inputWatermarkGauges.add(inputWatermarkGauge);

			List<GeneralValveOutputHandler.OperatorProxy> wrappers = new ArrayList<>();
			wrappers.add(new Proxy(headOperator));

			inputProcessor = new InputProcessorImpl(
					inputGatesCol,
					inputSerializers,
					inputWatermarkGauges,
					wrappers,
					this,
					configuration.getCheckpointMode(),
					getCheckpointLock(),
					getEnvironment().getIOManager(),
					getEnvironment().getTaskManagerInfo().getConfiguration(),
					getStreamStatusMaintainer(),
					headOperator,
					getEnvironment().getMetricGroup().getIOMetricGroup());
		}
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge::getValue);
	}

	@Override
	protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final InputProcessor inputProcessor = this.inputProcessor;

		while (running && inputProcessor.processInput()) {
			// all the work happens in the "processInput" method
		}
	}

	@Override
	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}

	/**
	 * Javadoc.
	 */
	private static class Proxy implements GeneralValveOutputHandler.OperatorProxy {

		private final OneInputStreamOperator<?, ?> operator;

		Proxy(OneInputStreamOperator<?, ?> operator) {
			this.operator = Preconditions.checkNotNull(operator);
		}

		@Override
		public void processLatencyMarker(LatencyMarker marker) throws Exception {
			operator.processLatencyMarker(marker);
		}

		@Override
		public void processElement(StreamRecord record) throws Exception {
			operator.setKeyContextElement1(record);
			operator.processElement(record);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.processWatermark(mark);
		}
	}
}
