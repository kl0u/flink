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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Javadoc.
 */
public class FileSinkExample {

	public static void main(final String[] args) throws Exception {
		final String outputPath = "file:///Users/kkloudas/Desktop/testingNewSink";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(4);
		env.enableCheckpointing(5000L);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(10L, TimeUnit.SECONDS)));

		final FileSink<String, Tuple2<Integer, Integer>> sink = FileSink
				.forRowFormat(new Path(outputPath), (Encoder<Tuple2<Integer, Integer>>) (element, stream) -> {
					PrintStream out = new PrintStream(stream);
					out.println(element.f1);
				})
				.withBucketAssigner(new KeyBucketAssigner())
				.withRollingPolicy(OnCheckpointRollingPolicy.build())
				.build();

		// generate data, shuffle, sink
		env.addSource(new Generator(10, 10, 60))
				.keyBy(0)
				.addSink(sink, "test-new-sink", Boundedness.CONTINUOUS_UNBOUNDED);

		env.execute("NewFileSinkProgram");
	}


	/**
	 * Use first field for buckets.
	 */
	public static final class KeyBucketAssigner implements BucketAssigner<Tuple2<Integer, Integer>, String> {

		private static final long serialVersionUID = 987325769970523326L;

		@Override
		public String getBucketId(final Tuple2<Integer, Integer> element, final Context context) {
			return String.valueOf(element.f0);
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * Data-generating source function.
	 */
	public static final class Generator implements SourceFunction<Tuple2<Integer, Integer>>, CheckpointedFunction {

		private static final long serialVersionUID = -2819385275681175792L;

		private final int numKeys;
		private final int idlenessMs;
		private final int recordsToEmit;

		private volatile int numRecordsEmitted = 0;
		private volatile boolean canceled = false;

		private ListState<Integer> state = null;

		Generator(final int numKeys, final int idlenessMs, final int durationSeconds) {
			this.numKeys = numKeys;
			this.idlenessMs = idlenessMs;

			this.recordsToEmit = ((durationSeconds * 1000) / idlenessMs) * numKeys;
		}

		@Override
		public void run(final SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			while (numRecordsEmitted < recordsToEmit) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < numKeys; i++) {
						ctx.collect(Tuple2.of(i, numRecordsEmitted));
						numRecordsEmitted++;
					}
				}
				Thread.sleep(idlenessMs);
			}

			while (!canceled) {
				Thread.sleep(50);
			}

		}

		@Override
		public void cancel() {
			canceled = true;
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(
					new ListStateDescriptor<Integer>("state", IntSerializer.INSTANCE));

			for (Integer i : state.get()) {
				numRecordsEmitted += i;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(numRecordsEmitted);
		}
	}
}
