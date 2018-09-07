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

package org.apache.flink.streaming.examples.nary;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedSideInputProcessFunction;
import org.apache.flink.streaming.api.functions.NonKeyedSideInputProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * Javadoc.
 */
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

		final ValueStateDescriptor<Integer> intState = new ValueStateDescriptor<>("testIntState", BasicTypeInfo.INT_TYPE_INFO);
		final ValueStateDescriptor<String> stringState = new ValueStateDescriptor<>("testStringState", BasicTypeInfo.STRING_TYPE_INFO);

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

		final DataStream<Integer> unionedInt = intStream.union(intStreamProc).keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		});

		// TODO: 8/29/18 how do we specify state???
		// TODO: 8/29/18 maybe through the context
		// todo what about chaining???

		// TODO: 8/29/18 test watermarks
		// TODO: 8/30/18 test with UNION
		// TODO: 8/30/18 test with side output

		stringStream
				.withSideInput(InputTag.withId("side1"), unionedInt, new KeyedSideInputProcessFunction<Integer, Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(Integer value, Context<Integer> ctx, Collector<String> out) throws Exception {
						System.out.println("SAW: " + getRuntimeContext().getState(intState).value() + " - PROCESSING: " + value);

						getRuntimeContext().getState(intState).update(value);
						out.collect("side1: " + value.toString());
					}
				})
				.withSideInput(InputTag.withId("side2"), doubleStream, new NonKeyedSideInputProcessFunction<Double, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(Double value, Context ctx, Collector<String> out) throws Exception {
						out.collect("side2: " + value.toString());
					}
				})
				.process(new NonKeyedSideInputProcessFunction<String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
						out.collect("main: " + value);
					}
				}, BasicTypeInfo.STRING_TYPE_INFO)
				.print();

		env.execute();

	}

}
