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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StreamNInputProcessor {


	private static final Logger LOG = LoggerFactory.getLogger(StreamNInputProcessor.class);

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	// order matters
	private final DeserializationDelegate<StreamElement>[] deserializationDelegates;

	private final CheckpointBarrierHandler barrierHandler;

	private final Object lock;

	// ---------------- Status and Watermark Valves ------------------

	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private StreamStatus[] inputStreamStatus;

	/**
	 * Valves that control how watermarks and stream statuses from the 2 inputs are forwarded.
	 */
	private StatusWatermarkValve[] statusWatermarkValves;

	/** Number of input channels the valves need to handle. */
	private final int[] numInputChannels;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to the correct channel index of the correct valve.
	 */
	private int currentChannel = -1;

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final TwoInputStreamOperator<?, ?, ?> streamOperator;

	// ---------------- Metrics ------------------

	private final WatermarkGauge[] inputWatermarkGauges;

	private Counter numRecordsIn;

	private boolean isFinished;

	@SuppressWarnings("unchecked")
	public StreamNInputProcessor(
			Collection<InputGate> inputGates1,
			Collection<InputGate> inputGates2,
			TypeSerializer<?> inputSerializer1,
			TypeSerializer<?> inputSerializer2,
			TwoInputStreamTask<?, ?, ?> checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<?, ?, ?> streamOperator,
			TaskIOMetricGroup metrics,
			WatermarkGauge input1WatermarkGauge,
			WatermarkGauge input2WatermarkGauge) throws IOException {

		final InputGate inputGate = InputGateUtil.createInputGate(inputGates1, inputGates2);

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);

		this.lock = checkNotNull(lock);

		this.deserializationDelegates = new DeserializationDelegate[2];

		StreamElementSerializer<?> ser1 = new StreamElementSerializer<>(inputSerializer1);
		deserializationDelegates[0] = new NonReusingDeserializationDelegate<>(ser1);

		StreamElementSerializer<?> ser2 = new StreamElementSerializer<>(inputSerializer2);
		deserializationDelegates[1] = new NonReusingDeserializationDelegate<>(ser2);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		// determine which unioned channels belong to input 1 and which belong to input 2
		this.numInputChannels = new int[2];
		numInputChannels[0] = 0;
		for (InputGate gate: inputGates1) {
			numInputChannels[0] += gate.getNumberOfInputChannels();
		}
		numInputChannels[1] = inputGate.getNumberOfInputChannels() - numInputChannels[0];
		System.out.println("DEF: " + numInputChannels[0] + " - " + numInputChannels[1]);

		this.inputStreamStatus = new StreamStatus[2];
		inputStreamStatus[0] = StreamStatus.ACTIVE;
		inputStreamStatus[1] = StreamStatus.ACTIVE;

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValves = new StatusWatermarkValve[2];
		statusWatermarkValves[0] = new StatusWatermarkValve(numInputChannels[0], new ForwardingValveOutputHandler1(streamOperator, lock));
		statusWatermarkValves[1] = new StatusWatermarkValve(numInputChannels[1], new ForwardingValveOutputHandler2(streamOperator, lock));

		this.inputWatermarkGauges = new WatermarkGauge[2];
		inputWatermarkGauges[0] = input1WatermarkGauge;
		inputWatermarkGauges[1] = input2WatermarkGauge;
		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);
	}

	private int deserializationDelegateIndex(int channelIndex) {
		if (currentChannel < numInputChannels[0]) {
			return 0;
		}
		return 1;
//
//		for (int counter = 0, i = 0; i < numInputChannels.length; i++) {
//			counter += numInputChannels[i];
//
//			if (channelIndex < counter) {
//				return i;
//			}
//		}
//		throw new IllegalStateException();
	}

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
			if (currentRecordDeserializer != null) {
				final int delegateIndex = deserializationDelegateIndex(currentChannel);
				final DeserializationDelegate<StreamElement> deserializationDelegate = deserializationDelegates[delegateIndex];
				final RecordDeserializer.DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				System.out.println(delegateIndex + " - " + currentChannel);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrWatermark = deserializationDelegate.getInstance();
					if (recordOrWatermark.isWatermark()) {
						statusWatermarkValves[delegateIndex].inputWatermark(recordOrWatermark.asWatermark(), currentChannel);
						continue;
					} else if (recordOrWatermark.isStreamStatus()) {
						statusWatermarkValves[delegateIndex].inputStreamStatus(recordOrWatermark.asStreamStatus(), currentChannel);
						continue;
					} else if (recordOrWatermark.isLatencyMarker()) {
						synchronized (lock) {
							if (delegateIndex == 0) {
								streamOperator.processLatencyMarker1(recordOrWatermark.asLatencyMarker());
							} else {
								streamOperator.processLatencyMarker2(recordOrWatermark.asLatencyMarker());
							}
						}
						continue;
					} else {
						synchronized (lock) {
							numRecordsIn.inc();
							if (delegateIndex == 0) {
								streamOperator.setKeyContextElement1(recordOrWatermark.asRecord());
								streamOperator.processElement1(recordOrWatermark.asRecord());
							} else {
								streamOperator.setKeyContextElement2(recordOrWatermark.asRecord());
								streamOperator.processElement2(recordOrWatermark.asRecord());
							}
						}
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {

				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());

				} else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}

	private class ForwardingValveOutputHandler1 implements StatusWatermarkValve.ValveOutputHandler {
		private final TwoInputStreamOperator<?, ?, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler1(final TwoInputStreamOperator<?, ?, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					inputWatermarkGauges[0].setCurrentWatermark(watermark.getTimestamp());
					operator.processWatermark1(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					inputStreamStatus[0] = streamStatus;

					// check if we need to toggle the task's stream status
					if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
						if (streamStatus.isActive()) {
							// we're no longer idle if at least one input has become active
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
						} else if (inputStreamStatus[1].isIdle()) {
							// we're idle once both inputs are idle
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}

	private class ForwardingValveOutputHandler2 implements StatusWatermarkValve.ValveOutputHandler {
		private final TwoInputStreamOperator<?, ?, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler2(final TwoInputStreamOperator<?, ?, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					inputWatermarkGauges[1].setCurrentWatermark(watermark.getTimestamp());
					operator.processWatermark2(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					inputStreamStatus[1] = streamStatus;

					// check if we need to toggle the task's stream status
					if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
						if (streamStatus.isActive()) {
							// we're no longer idle if at least one input has become active
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
						} else if (inputStreamStatus[0].isIdle()) {
							// we're idle once both inputs are idle
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}
}
