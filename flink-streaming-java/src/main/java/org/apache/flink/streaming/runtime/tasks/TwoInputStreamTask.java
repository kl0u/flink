/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.GeneralInputProcessor;
import org.apache.flink.streaming.runtime.io.GeneralValveOutputHandler;
import org.apache.flink.streaming.runtime.io.InputProcessor;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link StreamTask} for executing a {@link TwoInputStreamOperator}.
 */
@Internal
public class TwoInputStreamTask<IN1, IN2, OUT> extends StreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> {

	private InputProcessor inputProcessor;

	private volatile boolean running = true;

	private final WatermarkGauge input1WatermarkGauge;
	private final WatermarkGauge input2WatermarkGauge;
	private final MinWatermarkGauge minInputWatermarkGauge;

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	public TwoInputStreamTask(Environment env) {
		super(env);
		input1WatermarkGauge = new WatermarkGauge();
		input2WatermarkGauge = new WatermarkGauge();
		minInputWatermarkGauge = new MinWatermarkGauge(input1WatermarkGauge, input2WatermarkGauge);
	}

	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		ClassLoader userClassLoader = getUserCodeClassLoader();

		TypeSerializer<IN1> inputDeserializer1 = configuration.getTypeSerializerIn1(userClassLoader);
		TypeSerializer<IN2> inputDeserializer2 = configuration.getTypeSerializerIn2(userClassLoader);

		int numberOfInputs = configuration.getNumberOfInputs();

		ArrayList<InputGate> inputList1 = new ArrayList<InputGate>();
		ArrayList<InputGate> inputList2 = new ArrayList<InputGate>();

		List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = inEdges.get(i).getTypeNumber();
			InputGate reader = getEnvironment().getInputGate(i);
			switch (inputType) {
				case 1:
					inputList1.add(reader);
					break;
				case 2:
					inputList2.add(reader);
					break;
				default:
					throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		List<Collection<InputGate>> inputGates = new ArrayList<>();
		inputGates.add(inputList1);
		inputGates.add(inputList2);

		List<TypeSerializer<?>> inputSerializers = new ArrayList<>();
		inputSerializers.add(inputDeserializer1);
		inputSerializers.add(inputDeserializer2);

		List<WatermarkGauge> inputWatermarkGauges = new ArrayList<>();
		inputWatermarkGauges.add(input1WatermarkGauge);
		inputWatermarkGauges.add(input2WatermarkGauge);

		List<GeneralValveOutputHandler.OperatorProxy> wrappers = new ArrayList<>();
		wrappers.add(new FirstOperatorProxy(headOperator));
		wrappers.add(new SecondOperatorProxy(headOperator));

		this.inputProcessor = new GeneralInputProcessor(
				inputGates,
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

		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_1_WATERMARK, input1WatermarkGauge);
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_2_WATERMARK, input2WatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
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
	private static class FirstOperatorProxy implements GeneralValveOutputHandler.OperatorProxy {

		private final TwoInputStreamOperator<?, ?, ?> operator;

		FirstOperatorProxy(TwoInputStreamOperator<?, ?, ?> operator) {
			this.operator = Preconditions.checkNotNull(operator);
		}

		@Override
		public void processLatencyMarker(LatencyMarker marker) throws Exception {
			operator.processLatencyMarker1(marker);
		}

		@Override
		public void processElement(StreamRecord record) throws Exception {
			operator.setKeyContextElement1(record);
			operator.processElement1(record);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.processWatermark1(mark);
		}
	}

	/**
	 * Javadoc.
	 */
	private static class SecondOperatorProxy implements GeneralValveOutputHandler.OperatorProxy {

		private final TwoInputStreamOperator<?, ?, ?> operator;

		SecondOperatorProxy(TwoInputStreamOperator<?, ?, ?> operator) {
			this.operator = Preconditions.checkNotNull(operator);
		}

		@Override
		public void processLatencyMarker(LatencyMarker marker) throws Exception {
			operator.processLatencyMarker2(marker);
		}

		@Override
		public void processElement(StreamRecord record) throws Exception {
			operator.setKeyContextElement2(record);
			operator.processElement2(record);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			operator.processWatermark2(mark);
		}
	}
}
