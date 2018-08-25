package org.apache.flink.streaming.examples.nary;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.SideInputProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InputTag;

import java.util.ArrayList;
import java.util.List;

public class MultiInputOperatorExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		final List<Integer> intInput = new ArrayList<>();
		intInput.add(1);
		intInput.add(2);
		intInput.add(3);
		intInput.add(4);
		intInput.add(5);

		final List<String> stringInput = new ArrayList<>();
		stringInput.add("A");
		stringInput.add("B");
		stringInput.add("C");
		stringInput.add("D");
		stringInput.add("E");


		final List<Double> doubleInput = new ArrayList<>();
		doubleInput.add(100.0);
		doubleInput.add(200.0);
		doubleInput.add(300.0);
		doubleInput.add(400.0);
		doubleInput.add(500.0);

		final DataStream<Integer> intStream = env.fromCollection(intInput);
		final DataStream<String> stringStream = env.fromCollection(stringInput);
		final DataStream<Double> doubleStream = env.fromCollection(doubleInput);

		final DataStream<Integer> intStreamProc = intStream.flatMap(new FlatMapFunction<Integer, Integer>() {

			private static final long serialVersionUID = 1161640145726584736L;

			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				for (int i = 0; i < value; i++) {
					out.collect(value + 10);
				}
			}
		});

		final DataStream<Integer> unionedInt = intStream.union(intStreamProc);

		// TODO: 8/29/18 how do we specify state???
		// TODO: 8/29/18 maybe through the context
		// todo what about chaining???

		// TODO: 8/29/18 test watermarks
		// TODO: 8/30/18 test with UNION
		// TODO: 8/30/18 test with side output

		stringStream
				.withSideInput(InputTag.withId("side1"), unionedInt, new SideInputProcessFunction<Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void process(Integer value, Collector<String> out) throws Exception {
						out.collect("side1: " + value.toString());
					}
				})
				.withSideInput(InputTag.withId("side2"), doubleStream, new SideInputProcessFunction<Double, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void process(Double value, Collector<String> out) throws Exception {
						out.collect("side2: " + value.toString());
					}
				})
				.process(new SideInputProcessFunction<String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void process(String value, Collector<String> out) throws Exception {
						out.collect("main: " + value);
					}
				}, BasicTypeInfo.STRING_TYPE_INFO)
				.print();

		env.execute();

	}

}
