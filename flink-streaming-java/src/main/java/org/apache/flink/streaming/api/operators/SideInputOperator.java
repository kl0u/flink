package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.functions.SideInputProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

public class SideInputOperator<I, OUT> {

	// TODO: 8/29/18 could also put the keyselector here instead of the input processor

	private final MultiInputStreamOperator<OUT> operator;

	private final SideInputProcessFunction<I, OUT> function;

	private final Output<StreamRecord<OUT>> output;

	private TimestampedCollector<OUT> collector;

	SideInputOperator(
			final MultiInputStreamOperator<OUT> operator,
			final Output<StreamRecord<OUT>> output,
			final SideInputProcessFunction<I, OUT> function
	) {
		this.function = Preconditions.checkNotNull(function);
		this.operator = Preconditions.checkNotNull(operator);
		this.output = Preconditions.checkNotNull(output);
	}

	public void open() throws Exception {
		// TODO: 8/29/18 this is a Flatmap for now
		this.collector = new TimestampedCollector<>(output);
	}

	public StreamingRuntimeContext getRuntimeContext() {
		return operator.getRuntimeContext();
	}

	public void processElement(StreamRecord<I> element) throws Exception {
		collector.setTimestamp(element);
		function.process(element.getValue(), collector);
	}

	public void processWatermark(Watermark mark) throws Exception {
		operator.processWatermark(mark);
	}

	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		operator.processLatencyMarker(latencyMarker);
	}

	public void close() throws Exception {
		// TODO: 8/29/18 do nothing now
	}

}
