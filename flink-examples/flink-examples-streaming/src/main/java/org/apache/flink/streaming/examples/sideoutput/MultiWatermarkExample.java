package org.apache.flink.streaming.examples.sideoutput;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.SelectiveWatermarkAssigner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class MultiWatermarkExample {

	public static void main(String[] args) throws Exception {

		final List<Tuple2<Long, Long>> input = new ArrayList<>();
		input.add(new Tuple2<>(1L, 1L));
		input.add(new Tuple2<>(2L, 20L));
		input.add(new Tuple2<>(1L, 2L));
		input.add(new Tuple2<>(2L, 21L));
		input.add(new Tuple2<>(1L, 3L));
		input.add(new Tuple2<>(2L, 22L));
		input.add(new Tuple2<>(1L, 4L));
		input.add(new Tuple2<>(2L, 23L));

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1L);

		env.fromCollection(input).withWatermarkAssigners(
				new TestSelectiveAssigner() {
					private static final long serialVersionUID = 5205421884283200468L;

					@Override
					public String getId() {
						return "SLOW";
					}

					@Override
					public boolean select(Tuple2<Long, Long> field) {
						return field.f0 == 1L;
					}
				},
				new TestSelectiveAssigner() {
					private static final long serialVersionUID = -8011140393754572692L;

					@Override
					public String getId() {
						return "FAST";
					}

					@Override
					public boolean select(Tuple2<Long, Long> field) {
						return field.f0 == 2L;
					}
				}
		).keyBy(new KeySelector<Tuple2<Long,Long>, Long>() {
			private static final long serialVersionUID = -5418652699978930099L;

			@Override
			public Long getKey(Tuple2<Long, Long> value) {
				return value.f0;
			}

		}).process(new KeyedProcessFunction<Long, Tuple2<Long,Long>, String>() {
			private static final long serialVersionUID = -3769094390200355336L;

			@Override
			public void processElement(Tuple2<Long, Long> value, Context ctx, Collector<String> out) {
				System.out.println("HERE");
			}

		}).print();

		env.execute();

	}

	private abstract static class TestSelectiveAssigner implements SelectiveWatermarkAssigner<Tuple2<Long, Long>> {

		private static final long serialVersionUID = -1512741661391275024L;

		private long maxTimestamp = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(Tuple2<Long, Long> element, long previousElementTimestamp) {
			long timestamp = element.f1;
			maxTimestamp = timestamp;
			return timestamp;
		}

		@Override
		public Long getCurrentWatermark() {
			return maxTimestamp;
		}
	}
}
