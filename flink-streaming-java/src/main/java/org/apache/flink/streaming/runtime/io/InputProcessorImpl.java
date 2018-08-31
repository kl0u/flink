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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.InputConfig;
import org.apache.flink.streaming.runtime.tasks.MultiInputStreamTask;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class InputProcessorImpl<OUT, OP extends StreamOperator<OUT>> implements InputProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(InputProcessorImpl.class);

	private final ElementReader[] elementReaders;

	private final int[] channelRangeEndPerReader;

	private final CheckpointBarrierHandler barrierHandler;

	private final Object lock;

	private final OP streamOperator;

	// ---------------- Metrics ------------------

	private Counter numRecordsIn;

	private boolean isFinished;

	// ----------------- Runtime Fields --------------------

	private ElementReader currentElementReader;

	public InputProcessorImpl(
			final Map<InputTag, InputConfig> configs,
			final MultiInputStreamTask<OUT, OP> checkpointedTask,
			final CheckpointingMode checkpointMode,
			final Object lock,
			final IOManager ioManager,
			final ExecutionConfig executionConfig,
			final Configuration taskManagerConfig,
			final StreamStatusMaintainer streamStatusMaintainer,
			final OP streamOperator,
			final TaskIOMetricGroup metrics
	) throws IOException {

		Preconditions.checkArgument(configs != null && !configs.isEmpty());

		final Collection<Collection<InputGate>> inputGates = new ArrayList<>();
		for (InputConfig config : configs.values()) {
			inputGates.add(config.getInputGates());
		}

		final InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
				checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);
		this.lock = checkNotNull(lock);
		this.streamOperator = Preconditions.checkNotNull(streamOperator);

		this.elementReaders = new ElementReader[configs.size()];
		this.channelRangeEndPerReader = new int[configs.size()];

		final GeneralValveOutputHandler[] handlers = new GeneralValveOutputHandler[configs.size()];
		int channelOffset = 0;

		int j = 0;
		for (Map.Entry<InputTag, InputConfig> e : configs.entrySet()) {
			final InputTag tag = e.getKey();
			final InputConfig config = e.getValue();

			final Collection<InputGate> gates = config.getInputGates();
			final TypeSerializer<?> serializer = config.getInputTypeInfo().createSerializer(Preconditions.checkNotNull(executionConfig));
			final GeneralValveOutputHandler.OperatorProxy wrapper = config.createOperatorProxy(tag, streamOperator);
			final WatermarkGauge watermarkGauge = config.getWatermarkGauge();

			int noOfChannels = 0;
			for (InputGate gate : gates) {
				noOfChannels += gate.getNumberOfInputChannels();
			}

			final DeserializationDelegate<StreamElement> deserializationDelegate =
					new NonReusingDeserializationDelegate<>(new StreamElementSerializer<>(serializer));
			final GeneralValveOutputHandler handler =
					new GeneralValveOutputHandler(wrapper, streamStatusMaintainer, watermarkGauge, lock);
			handlers[j] = handler;

			elementReaders[j] = new ElementReader(
					wrapper,
					initializeRecordDeserializers(ioManager, noOfChannels),
					deserializationDelegate,
					handler,
					noOfChannels,
					channelOffset);

			channelOffset += noOfChannels;
			channelRangeEndPerReader[j] = channelOffset;
			j++;
		}

		for (int i = 0; i < handlers.length; i++) {
			final GeneralValveOutputHandler[] others = cleanHandlerList(handlers, i);
			handlers[i].addLocalInputHandlers(others);
		}
		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);
	}

	// TODO: 8/24/18 this needs extensive testing. Small list, remove boundaries...
	private static GeneralValveOutputHandler[] cleanHandlerList(GeneralValveOutputHandler[] originalList, int indexToRemove) {
		final GeneralValveOutputHandler[] others = new GeneralValveOutputHandler[originalList.length - 1];
		if (indexToRemove == 0) {
			System.arraycopy(originalList, 1, others, 0, others.length);
		} else if (indexToRemove == originalList.length - 1) {
			System.arraycopy(originalList, 0, others, 0, others.length);
		} else {
			System.arraycopy(originalList, 0, others, 0, indexToRemove - 1);
			System.arraycopy(originalList, indexToRemove + 1, others, indexToRemove, originalList.length - indexToRemove - 1);
		}
		return others;
	}

	private static RecordDeserializer<DeserializationDelegate<StreamElement>>[] initializeRecordDeserializers(
			final IOManager ioManager,
			final int noOfInputChannels) {

		@SuppressWarnings("unchecked") final RecordDeserializer<DeserializationDelegate<StreamElement>>[] deserializers =
				new SpillingAdaptiveSpanningRecordDeserializer[noOfInputChannels];

		for (int i = 0; i < deserializers.length; i++) {
			deserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
		}

		return deserializers;
	}

	@Override
	public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}

		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}

		while (true) {
			if (currentElementReader != null && currentElementReader.isReadyToReadElement()) {
				final StreamElement element = currentElementReader.readElement();
				if (element != null) {
					if (element.isWatermark()) {
						currentElementReader.processWatermark(element.asWatermark());
						continue;
					} else if (element.isStreamStatus()) {
						currentElementReader.processStreamStatus(element.asStreamStatus());
						continue;
					} else if (element.isLatencyMarker()) {
						synchronized (lock) {
							currentElementReader.processLatencyMarker(element.asLatencyMarker());
						}
						continue;
					} else {
						final StreamRecord record = element.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							currentElementReader.processElement(record);
						}
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					getAndPrepareElementReaderForReading(bufferOrEvent);
				} else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			} else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}

	private void getAndPrepareElementReaderForReading(final BufferOrEvent buffer) throws IOException {
		final int currentChannel = buffer.getChannelIndex();
		for (int index = 0; index < channelRangeEndPerReader.length; index++) {
			if (currentChannel < channelRangeEndPerReader[index]) {
				currentElementReader = elementReaders[index];
				currentElementReader.prepareToReadElement(buffer);
				return;
			}
		}
		throw new IllegalStateException("Channel not covered by reader.");
	}

	@Override
	public void cleanup() throws Exception {
		for (ElementReader reader : elementReaders) {
			reader.cleanup();
		}
		barrierHandler.cleanup();
	}
}
