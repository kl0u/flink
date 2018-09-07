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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.streaming.api.graph.SideInputEdgeInfo;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;


/**
 * Test harness for testing a {@link MultiInputStreamTask} with a single input.
 *
 * <p>This mock Invokable provides the task with a basic runtime context and allows pushing elements
 * and watermarks into the task. {@link #getOutput()} can be used to get the emitted elements
 * and events. You are free to modify the retrieved list.
 *
 * <p>After setting up everything the Task can be invoked using {@link #invoke()}. This will start
 * a new Thread to execute the Task. Use {@link #waitForTaskCompletion()} to wait for the Task
 * thread to finish. Use {@link #processElement} to send elements to the task. Use
 * {@link #processEvent(AbstractEvent)} to send events to the task.
 * Before waiting for the task to finish you must call {@link #endInput()} to signal to the task
 * that data entry is finished.
 *
 * <p>When Elements or Events are offered to the Task they are put into a queue. The input gates
 * of the Task notifyNonEmpty from this queue. Use {@link #waitForInputProcessing()} to wait until all
 * queues are empty. This must be used after entering some elements before checking the
 * desired output.
 */
public class OneInputStreamTaskTestHarness<IN, OUT> extends StreamTaskTestHarness<OUT> {

	private TypeInformation<IN> inputType;
	private TypeSerializer<IN> inputSerializer;

	private final KeySelector<IN, ?> keySelector;
	private final TypeInformation<?> keyType;

	public OneInputStreamTaskTestHarness(
			Function<Environment, ? extends StreamTask<OUT, ?>> taskFactory,
			int numInputGates,
			int numInputChannelsPerGate,
			TypeInformation<IN> inputType,
			TypeInformation<OUT> outputType) {
		this(taskFactory, numInputGates, numInputChannelsPerGate, inputType, outputType, null, null);
	}

	/**
	 * Creates a test harness with the specified number of input gates and specified number
	 * of channels per input gate.
	 */
	public <K> OneInputStreamTaskTestHarness(
			Function<Environment, ? extends StreamTask<OUT, ?>> taskFactory,
			int numInputGates,
			int numInputChannelsPerGate,
			TypeInformation<IN> inputType,
			TypeInformation<OUT> outputType,
			KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType) {

		super(taskFactory, outputType);

		this.inputType = inputType;
		inputSerializer = inputType.createSerializer(executionConfig);

		this.numInputGates = numInputGates;
		this.numInputChannelsPerGate = numInputChannelsPerGate;

		if (keySelector != null) {
			ClosureCleaner.clean(keySelector, false);
			this.keySelector =  keySelector;
			this.keyType = Preconditions.checkNotNull(keyType);
			streamConfig.setStateKeySerializer(keyType.createSerializer(executionConfig));
		} else {
			this.keySelector = null;
			this.keyType = null;
		}
	}

	public OneInputStreamTaskTestHarness(
			Function<Environment, ? extends StreamTask<OUT, ?>> taskFactory,
			TypeInformation<IN> inputType,
			TypeInformation<OUT> outputType) {
		this(taskFactory, inputType, outputType, null, null);
	}

	/**
	 * Creates a test harness with one input gate that has one input channel.
	 */
	public <K> OneInputStreamTaskTestHarness(
			Function<Environment, ? extends StreamTask<OUT, ?>> taskFactory,
			TypeInformation<IN> inputType,
			TypeInformation<OUT> outputType,
			KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType) {

		this(taskFactory, 1, 1, inputType, outputType, keySelector, keyType);
	}

	@Override
	protected void initializeInputs() throws IOException, InterruptedException {
		inputGates = new StreamTestSingleInputGate[numInputGates];

		final List<StreamEdge> inPhysicalEdges = new ArrayList<>();

		final StreamOperator<IN> dummyOperator = new AbstractStreamOperator<IN>() {
			private static final long serialVersionUID = 1L;
		};

		StreamNode sourceVertexDummy = new StreamNode(null, 0, "default group", null, dummyOperator, "source dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(null, 1, "default group", null, dummyOperator, "target dummy", new LinkedList<>(), SourceStreamTask.class);

		for (int i = 0; i < numInputGates; i++) {
			inputGates[i] = new StreamTestSingleInputGate<IN>(
				numInputChannelsPerGate,
				bufferSize,
				inputSerializer);

			final StreamEdge edge = new StreamEdge(
					sourceVertexDummy,
					targetVertexDummy,
					new ArrayList<>(),
					new BroadcastPartitioner<>(),
					null,
					new SideInputEdgeInfo(
							InputTag.MAIN_INPUT_TAG,
							inputType,
							keySelector,
							keyType)
					);
			inPhysicalEdges.add(edge);
			mockEnv.addInputGate(inputGates[i].getInputGate());
		}

		streamConfig.setInPhysicalEdges(inPhysicalEdges);
		streamConfig.setNumberOfInputs(numInputGates);
		streamConfig.setTypeSerializerIn1(inputSerializer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public MultiInputStreamTask<OUT, ?> getTask() {
		return (MultiInputStreamTask<OUT, ?>) super.getTask();
	}
}

