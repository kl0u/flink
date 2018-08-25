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
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 * todo can I have a mapping of channel index to side input tag???
 */
public class InputProcessorImpl implements InputProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(InputProcessorImpl.class);

	private final ElementReader[] elementReaders;

	private final int[] channelRangeEndPerReader;

	private final CheckpointBarrierHandler barrierHandler;

	private final Object lock;

	private final StreamOperator<?> streamOperator;

	// ---------------- Metrics ------------------

	private Counter numRecordsIn;

	private boolean isFinished;

	// ----------------- Runtime Fields --------------------

	private ElementReader currentElementReader;

	@SuppressWarnings("unchecked")
	public InputProcessorImpl(
			List<Collection<InputGate>> inputGates, //  todo the 4 lists should become sth like configuration object
			List<TypeSerializer<?>> inputSerializers,
			List<WatermarkGauge> input1WatermarkGauges,
			List<GeneralValveOutputHandler.OperatorProxy> wrappers,
			StreamTask<?, ?> checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			StreamOperator<?> streamOperator, // TODO: 8/24/18 this will be removed
			TaskIOMetricGroup metrics) throws IOException {

		Preconditions.checkState(
				inputGates.size() == inputSerializers.size() &&
						inputGates.size() == input1WatermarkGauges.size() &&
						inputGates.size() == wrappers.size()
		);

		final InputGate inputGate = InputGateUtil.createInputGate(checkNotNull(inputGates));

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
				checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);
		this.lock = checkNotNull(lock);
		this.streamOperator = checkNotNull(streamOperator);

		this.elementReaders = new ElementReader[inputGates.size()];
		this.channelRangeEndPerReader = new int[inputGates.size()];

		final GeneralValveOutputHandler[] handlers = new GeneralValveOutputHandler[inputGates.size()];
		int channelOffset = 0;

		for (int i = 0; i < inputGates.size(); i++) {

			// TODO: 8/24/18 these should be passed as readerConf
			final Collection<InputGate> gates = inputGates.get(i);
			int noOfChannels = 0;
			for (InputGate gate : gates) {
				noOfChannels += gate.getNumberOfInputChannels();
			}
			final GeneralValveOutputHandler.OperatorProxy wrapper = wrappers.get(i);
			final TypeSerializer<?> serializer = inputSerializers.get(i);
			final WatermarkGauge watermarkGauge = input1WatermarkGauges.get(i);

			final DeserializationDelegate<StreamElement> deserializationDelegate =
					new NonReusingDeserializationDelegate<>(new StreamElementSerializer<>(serializer));
			final GeneralValveOutputHandler handler =
					new GeneralValveOutputHandler(wrapper, streamStatusMaintainer, watermarkGauge, lock);
			handlers[i] = handler;

			elementReaders[i] = new ElementReader(
					wrapper,
					initializeRecordDeserializers(ioManager, noOfChannels),
					deserializationDelegate,
					handler,
					noOfChannels,
					channelOffset);

			channelOffset += noOfChannels;
			channelRangeEndPerReader[i] = channelOffset;
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

	private static RecordDeserializer<DeserializationDelegate<StreamElement>>[] initializeRecordDeserializers(final IOManager ioManager, final int noOfInputChannels) {

		@SuppressWarnings("unchecked")
		final RecordDeserializer<DeserializationDelegate<StreamElement>>[] deserializers =
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
