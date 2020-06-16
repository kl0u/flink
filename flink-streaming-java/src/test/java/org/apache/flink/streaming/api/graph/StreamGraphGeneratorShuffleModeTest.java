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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.util.NoOpIntMap;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * A test class for testing setting a correct {@link ShuffleMode} by {@link StreamGraphGenerator}.
 */
public class StreamGraphGeneratorShuffleModeTest {
	public static final CoMapFunction<Integer, Integer, Object> NO_OP_CO_MAPPER =
		new CoMapFunction<Integer, Integer, Object>() {
			@Override
			public Object map1(Integer value) throws Exception {
				return null;
			}

			@Override
			public Object map2(Integer value) throws Exception {
				return null;
			}
		};

	@Test
	public void testBoundedSourcesShuffleHaveBatchShuffleMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> source = env.continuousSource(
			new MockSource(Boundedness.BOUNDED, 2),
			WatermarkStrategy.noWatermarks(),
			"bounded");

		DataStream<Integer> keyedResult = source.keyBy(value -> value).map(new NoOpIntMap());
		keyedResult.addSink(new DiscardingSink<>());

		StreamGraph graph = env.getStreamGraph();
		StreamNode keyedResultNode = graph.getStreamNode(keyedResult.getId());
		assertThat(keyedResultNode.getInEdges().get(0).getShuffleMode(), equalTo(ShuffleMode.BATCH));
	}

	@Test
	public void testUnboundedSourcesShuffleHaveBatchShuffleMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> source = env.continuousSource(
			new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 2),
			WatermarkStrategy.noWatermarks(),
			"unbounded");

		DataStream<Integer> keyedResult = source.keyBy(value -> value).map(new NoOpIntMap());
		keyedResult.addSink(new DiscardingSink<>());

		StreamGraph graph = env.getStreamGraph();
		StreamNode keyedResultNode = graph.getStreamNode(keyedResult.getId());
		assertThat(keyedResultNode.getInEdges().get(0).getShuffleMode(), equalTo(ShuffleMode.UNDEFINED));
	}

	@Test
	public void testMixedSourcesShuffleHaveBatchShuffleMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> boundedSource = env.continuousSource(
			new MockSource(Boundedness.BOUNDED, 2),
			WatermarkStrategy.noWatermarks(),
			"bounded");
		DataStreamSource<Integer> continuousSource = env.continuousSource(
			new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 2),
			WatermarkStrategy.noWatermarks(),
			"unbounded");

		SingleOutputStreamOperator<Integer> firstShuffle = boundedSource
			.keyBy(value -> value)
			.map(new NoOpIntMap());

		SingleOutputStreamOperator<Object> secondShuffle = firstShuffle.connect(continuousSource)
			.keyBy(value -> value, value -> value)
			.map(NO_OP_CO_MAPPER);
		secondShuffle.addSink(new DiscardingSink<>());

		StreamGraph graph = env.getStreamGraph();
		StreamNode firstShuffleNode = graph.getStreamNode(firstShuffle.getId());
		StreamNode secondShuffleNode = graph.getStreamNode(secondShuffle.getId());

		assertThat(firstShuffleNode.getInEdges().get(0).getShuffleMode(), equalTo(ShuffleMode.BATCH));
		assertThat(secondShuffleNode.getInEdges().get(0).getShuffleMode(), equalTo(ShuffleMode.BATCH));
		assertThat(secondShuffleNode.getInEdges().get(1).getShuffleMode(), equalTo(ShuffleMode.UNDEFINED));
	}
}
