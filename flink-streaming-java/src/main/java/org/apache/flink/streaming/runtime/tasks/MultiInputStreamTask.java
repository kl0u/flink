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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.SideInputEdgeInfo;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.InputProcessor;
import org.apache.flink.streaming.runtime.io.InputProcessorImpl;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Javadoc.
 */
@Internal
public class MultiInputStreamTask<OUT, OP extends StreamOperator<OUT>> extends StreamTask<OUT, OP> {

	private InputProcessor inputProcessor;

	private volatile boolean running = true;

	public MultiInputStreamTask(Environment env) {
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
	public MultiInputStreamTask(
			Environment env,
			@Nullable ProcessingTimeService timeProvider) {
		super(env, timeProvider);
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

		final WatermarkGauge[] gauges = new WatermarkGauge[inputConfigs.size()];
		int i = 0;
		for (InputConfig config : inputConfigs.values()) {
			final WatermarkGauge gauge = config.getWatermarkGauge();
			gauges[i++] = gauge;
			final String metricName = MetricNames.getInputWatermarkGaugeName(i);
			headOperator.getMetricGroup().gauge(metricName, gauge);
		}

		final MinWatermarkGauge minInputWatermarkGauge = new MinWatermarkGauge(gauges);

		this.inputProcessor = new InputProcessorImpl<>(
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

		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
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
