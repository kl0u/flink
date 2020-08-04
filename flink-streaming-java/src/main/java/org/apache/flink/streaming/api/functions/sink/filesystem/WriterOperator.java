package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class WriterOperator<WriterID, IN, OUT extends Committable, WriterStateT extends WriterState<WriterID>, CommonStateT>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT>, ProcessingTimeCallback {

	private static final ListStateDescriptor<byte[]> WRITER_STATE_DESC =
			new ListStateDescriptor<>("writer-state", BytePrimitiveArraySerializer.INSTANCE);

	private final long writerCheckInterval;
	
	private final WriterAssigner<IN, WriterID> writerAssigner;

	private final Function<WriterContext, Writer<WriterID, IN, OUT, WriterStateT, CommonStateT>> sinkWriterFactory;

	private final Map<WriterID, Writer<WriterID, IN, OUT, WriterStateT, CommonStateT>> activeWriters;

	private transient AssignerContext assignerContext;

	private transient ListState<byte[]> commonWriterStateStore;

	private transient ListState<byte[]> individualWriterStateStore;

	private transient long lastWatermark;

	private transient SimpleWriterContext writerContext;

	private transient WriterOutput<OUT> writerOutput;

	private final Map<WriterID, WriterStateT> states = new HashMap<>();

	private final List<CommonStateT> sharedState = new ArrayList<>();

	public WriterOperator(
			final long writerCheckIntervalMs,
			final WriterAssigner<IN, WriterID> writerAssigner,
			final Function<WriterContext, Writer<WriterID, IN, OUT, WriterStateT, CommonStateT>> writerFactory) {
		this.writerCheckInterval = writerCheckIntervalMs;
		this.writerAssigner = checkNotNull(writerAssigner);
		this.sinkWriterFactory = checkNotNull(writerFactory);
		this.activeWriters = new HashMap<>();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		this.lastWatermark = Long.MIN_VALUE;
		this.assignerContext = new AssignerContext();
		this.individualWriterStateStore = context.getOperatorStateStore().getListState(WRITER_STATE_DESC);
		this.commonWriterStateStore = context.getOperatorStateStore().getUnionListState(WRITER_STATE_DESC);

		if (context.isRestored()) {

			final WriterContext<WriterID> c = createWriterContext();
			final Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> w = sinkWriterFactory.apply(c);
			final SimpleVersionedSerializer<WriterStateT> serializer = w.getSubtaskStateSerializer();
			final SimpleVersionedSerializer<CommonStateT> commonStateSerializer = w.getCommonStateSerializer();

			for (byte[] state : individualWriterStateStore.get()) {
				final WriterStateT deserializedState = SimpleVersionedSerialization
						.readVersionAndDeSerialize(serializer, state);
				final WriterID writerID = deserializedState.getWriterID();

				final WriterStateT writerState = states.get(writerID);
				if (writerState != null) {
					writerState.merge(deserializedState);
				} else {
					states.put(writerID, deserializedState);
				}
			}

			for (byte[] state : commonWriterStateStore.get()) {
				final CommonStateT commonState = SimpleVersionedSerialization
						.readVersionAndDeSerialize(commonStateSerializer, state);
				sharedState.add(commonState);
			}
		}
	}
	
	@Override
	public void open() throws Exception {
		super.open();

		this.writerContext = new SimpleWriterContext();

		this.writerOutput = new SimpleWriterOutput<>(output);
		initializeRestoredWriters();

		final long currentTime = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(currentTime + writerCheckInterval, this);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		final long currentTime = getProcessingTimeService().getCurrentProcessingTime();
		for (Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer : activeWriters.values()) {
			writer.onPeriodicCheck(currentTime, writerOutput);
		}
		getProcessingTimeService().registerTimer(currentTime + writerCheckInterval, this);
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		individualWriterStateStore.clear();
		commonWriterStateStore.clear();

		for (Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer : activeWriters.values()) {
			snapshotSubtaskState(writer);
			snapshotCommonState(writer);
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		final Iterator<Map.Entry<WriterID, Writer<WriterID, IN, OUT, WriterStateT, CommonStateT>>> activeWriterIt =
				activeWriters.entrySet().iterator();

		while (activeWriterIt.hasNext()) {
			// TODO: 05.08.20 maybe we need to have a method in the writer as well.
			final Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer = activeWriterIt.next().getValue();
			if (!writer.isActive()) {
				activeWriterIt.remove();
			}
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		this.lastWatermark = mark.getTimestamp();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final Long timestamp = element.getTimestamp();
		final long watermark = lastWatermark;
		final long currentTime = getProcessingTimeService().getCurrentProcessingTime();

		final WriterID writerId = getWriterID(element, timestamp, watermark, currentTime);
		writeElement(element, timestamp, watermark, currentTime, writerId);

		// TODO: 06.08.20 here we may need to somhow handle the common state
	}

	private void initializeRestoredWriters() throws Exception {
		for (Map.Entry<WriterID, WriterStateT> entry : states.entrySet()) {
			final WriterContext writerContext = createWriterContext();
			final Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer = sinkWriterFactory.apply(writerContext);
			activeWriters.put(entry.getKey(), writer);
			writer.resume(entry.getValue(), sharedState, writerOutput);
		}

		// be gc friendly
		states.clear();
		sharedState.clear();
	}

	// TODO: 06.08.20 checkpoint
	private WriterID getWriterID(StreamRecord<IN> element, Long timestamp, long watermark, long currentTime) {
		assignerContext.update(timestamp, watermark, currentTime);
		return writerAssigner.getWriterId(element.getValue(), assignerContext);
	}

	private void writeElement(StreamRecord<IN> element, Long timestamp, long watermark, long currentTime, WriterID writerId) throws Exception {
		final Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer = getOrCreateWriter(writerId);
		writerContext.update(timestamp, watermark, currentTime);
		writer.write(element.getValue(), writerContext, writerOutput);
	}

	private Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> getOrCreateWriter(final WriterID writerID) {
		Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer = activeWriters.get(writerID);
		if (writer == null) {
			writer = sinkWriterFactory.apply(createWriterContext());  // TODO: 06.08.20 here we may need the commonstate or sth
			activeWriters.put(writerID, writer);
		}
		return writer;
	}

	private void snapshotCommonState(Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer) throws Exception {
		final SimpleVersionedSerializer<CommonStateT> commonStateSerializer =
				writer.getCommonStateSerializer();
		final CommonStateT commonState = writer.getCommonState();
		final byte[] serializedCommonState = SimpleVersionedSerialization
				.writeVersionAndSerialize(commonStateSerializer, commonState);

		commonWriterStateStore.add(serializedCommonState);
	}

	// TODO: 07.08.20 the preBarrier would be nicer than the getState with output
	private void snapshotSubtaskState(Writer<WriterID, IN, OUT, WriterStateT, CommonStateT> writer) throws Exception {
		final SimpleVersionedSerializer<WriterStateT> serializer = writer.getSubtaskStateSerializer();
		final WriterStateT writerState = writer.getSubtaskState(writerOutput);
		final byte[] serializedSubtaskState = SimpleVersionedSerialization
				.writeVersionAndSerialize(serializer, writerState);

		individualWriterStateStore.add(serializedSubtaskState);
	}

	private WriterContext<WriterID> createWriterContext() {
		return new WriterContext<WriterID>() {
			@Override
			public int getSubtaskId() {
				return getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public MetricGroup metricGroup() {
				return getMetricGroup();
			}
		};
	}

	private static class SimpleWriterOutput<OUT extends Committable> implements WriterOutput<OUT> {

		private final Output<StreamRecord<OUT>> output;

		public SimpleWriterOutput(Output<StreamRecord<OUT>> output) {
			this.output = checkNotNull(output);
		}

		@Override
		public void collect(OUT element) {
			if (element != null) {
				this.output.collect(new StreamRecord<>(element));
			}
		}
	}

	private static class SimpleWriterContext implements Writer.Context {

		@Nullable
		private Long elementTimestamp;

		private long currentWatermark;

		private long currentProcessingTime;

		SimpleWriterContext() {
			this.elementTimestamp = null;
			this.currentWatermark = Long.MIN_VALUE;
			this.currentProcessingTime = Long.MIN_VALUE;
		}

		void update(@Nullable Long elementTimestamp, long watermark, long processingTime) {
			this.elementTimestamp = elementTimestamp;
			this.currentWatermark = watermark;
			this.currentProcessingTime = processingTime;
		}

		@Override
		public long currentProcessingTime() {
			return currentProcessingTime;
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		@Nullable
		public Long timestamp() {
			return elementTimestamp;
		}
	}
}
