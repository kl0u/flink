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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.graph.SideInputEdgeInfo;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.MultiInputStreamOperator;
import org.apache.flink.streaming.runtime.io.InputProcessor;
import org.apache.flink.streaming.runtime.io.MultiInputProcessorImpl;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Javadoc.
 */
public class MultiInputStreamTask<OUT> extends StreamTask<OUT, MultiInputStreamOperator<OUT>> {

	// TODO: 8/24/18 also add the watermarkGauges
	private InputProcessor inputProcessor;

	private volatile boolean running = true;

	public MultiInputStreamTask(Environment env) {
		super(env);
	}

	@Override
	protected void init() throws Exception {

		// we use the userClassLoader here because we also have UDFs in the edges
		final ClassLoader userClassLoader = getUserCodeClassLoader();
		final List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

		final Map<InputTag, InputConfig> inputConfigs = new HashMap<>();

		for (int i = 0; i < configuration.getNumberOfInputs(); i++) {
			final SideInputEdgeInfo<?, ?, ?> inputInfo = inEdges.get(i).getSideInputInfo();
			Preconditions.checkState(inputInfo != null); // the input tag is never null.

			final InputTag inputTag = inputInfo.getInputTag();
			final TypeInformation<?> inputTypeInfo = inputInfo.getInputTypeInfo();
			final KeySelector<?, ?> keySelector = inputInfo.getKeySelector();

			InputConfig config = inputConfigs.get(inputTag);

			if (config != null && !Objects.equals(config.getInputTypeInfo(), inputTypeInfo)) {
				// sanity check (the equality is added for unions)
				throw new IllegalStateException("Tagged Input \"" + inputTag + "\" already seen with different element type.");
			}

			if (config == null) {
				config = new InputConfig(inputTypeInfo, keySelector);
				inputConfigs.put(inputTag, config);
			}
			config.addInputGate(getEnvironment().getInputGate(i));
		}
		
		this.inputProcessor = new MultiInputProcessorImpl<>(
				inputConfigs,
				this,
				configuration.getCheckpointMode(),
				getCheckpointLock(),
				getEnvironment().getIOManager(),
				getExecutionConfig(),
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getStreamStatusMaintainer(),
				headOperator,
				getEnvironment().getMetricGroup().getIOMetricGroup());

//		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
//		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_1_WATERMARK, input1WatermarkGauge);
//		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_2_WATERMARK, input2WatermarkGauge);
//		// wrap watermark gauge since registered metrics must be unique
//		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
	}

	@Override
	protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final InputProcessor processor = inputProcessor;

		while (running && processor.processInput()) {
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
}
