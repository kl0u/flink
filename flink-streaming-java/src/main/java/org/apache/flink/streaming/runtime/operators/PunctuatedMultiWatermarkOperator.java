package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.streaming.api.functions.SelectivePunctuatedWatermarkAssigner;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PunctuatedMultiWatermarkOperator<IN>
		extends AbstractUdfStreamOperator<IN, PunctuatedMultiWatermarkOperator.PunctuatedMultiWatermarkAssigner<IN>>
		implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1509538640986248046L;

	private transient Map<String, Long> currentWatermarks;

	public PunctuatedMultiWatermarkOperator(final List<SelectivePunctuatedWatermarkAssigner<IN>> assigners) {
		super(Preconditions.checkNotNull(new PunctuatedMultiWatermarkAssigner<>(assigners)));
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void open() throws Exception {
		super.open();

		// for each tag we monitor the
		this.currentWatermarks = new HashMap<>();
		for (String tag: userFunction.getTags()) {
			currentWatermarks.put(tag, Long.MIN_VALUE);
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final IN value = element.getValue();
		final long newTimestamp = userFunction.extractTimestamp(
				value,
				element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		final String tag = userFunction.getTag(value);
		output.collect(element.replace(element.getValue(), newTimestamp, tag));

		final Watermark nextWatermark = userFunction.checkAndGetNextWatermark(tag, value, newTimestamp);
		if (nextWatermark != null && nextWatermark.getTimestamp() > currentWatermarks.get(tag)) {
			currentWatermarks.put(tag, nextWatermark.getTimestamp());
			output.emitWatermark(nextWatermark);
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE) {
			for (String tag : userFunction.getTags()) {
				currentWatermarks.put(tag, Long.MAX_VALUE);

				final Watermark newWatermark = new Watermark(Long.MAX_VALUE);
				newWatermark.setTag(tag);
				output.emitWatermark(newWatermark);
			}
		}
	}

	/**
	 * Javadoc.
	 * todo deduplicate code with the periodic one.
	 */
	public static class PunctuatedMultiWatermarkAssigner<T> implements TimestampAssigner<T> {

		private static final long serialVersionUID = 4998222228683986485L;

		private final Map<String, SelectivePunctuatedWatermarkAssigner<T>> watermarkAssigners;

		public PunctuatedMultiWatermarkAssigner(List<SelectivePunctuatedWatermarkAssigner<T>> assigners) {
			this.watermarkAssigners = Collections.unmodifiableMap(
					Preconditions.checkNotNull(loadAssigners(assigners))
			);
		}

		private Map<String, SelectivePunctuatedWatermarkAssigner<T>> loadAssigners(List<SelectivePunctuatedWatermarkAssigner<T>> assigners) {
			Map<String, SelectivePunctuatedWatermarkAssigner<T>> tmp = new HashMap<>(assigners.size());
			for (SelectivePunctuatedWatermarkAssigner<T> assigner : assigners) {
				tmp.put(assigner.getTag(), assigner);
			}
			return tmp;
		}

		public String getTag(T element) {
			for (SelectivePunctuatedWatermarkAssigner<T> assigner : watermarkAssigners.values()) {
				if (assigner.select(element)) {
					return assigner.getTag();
				}
			}
			// TODO: 6/7/18 we should have a default
			throw new FlinkRuntimeException("Uncategorized element " + element);
		}

		public Set<String> getTags() {
			return Collections.unmodifiableSet(watermarkAssigners.keySet());
		}

		public Watermark checkAndGetNextWatermark(String tag, T lastElement, long extractedTimestamp) {
			SelectivePunctuatedWatermarkAssigner<T> assigner = watermarkAssigners.get(tag);
			if (assigner == null) {
				// TODO: 6/7/18 we should have a default
				throw new FlinkRuntimeException("Uncategorized element " + lastElement);
			}

			final Watermark watermark = assigner.checkAndGetNextWatermark(lastElement, extractedTimestamp);
			if (watermark != null) {
				watermark.setTag(tag);
			}
			return watermark;
		}


		@Override
		public long extractTimestamp(T element, long previousElementTimestamp) {
			for (SelectivePunctuatedWatermarkAssigner<T> assigner: watermarkAssigners.values()) {
				if (assigner.select(element)) {
					return assigner.extractTimestamp(element, previousElementTimestamp);
				}
			}
			// TODO: 6/7/18 we should have a default
			throw new FlinkRuntimeException("Uncategorized element " + element);
		}
	}
}
