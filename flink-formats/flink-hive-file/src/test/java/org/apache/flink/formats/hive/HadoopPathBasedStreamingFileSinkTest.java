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

package org.apache.flink.formats.hive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.runtime.fs.hdfs.HadoopDataOutputStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.HadoopPathBasedBulkFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.PathBasedBulkWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class HadoopPathBasedStreamingFileSinkTest extends AbstractTestBase {

	@Rule
	public final Timeout timeoutPerTest = Timeout.seconds(2000);

	@Test
	public void testWriteFile() throws Exception {

		File folder = new File("/tmp/aaa");

		List<String> data = Arrays.asList(
			"first line",
			"second line",
			"third line");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(100);

		DataStream<String> stream = env.addSource(
			new FiniteTestSource<>(data), TypeInformation.of(String.class));

		Configuration configuration = new Configuration();

		HadoopPathBasedBulkFormatBuilder<String, String, ?> builder =
			new HadoopPathBasedBulkFormatBuilder<>(
				new Path("file://" + folder.getAbsolutePath()),
				new TestHadoopPathBasedBulkWriterFactory(),
				configuration,
				new DateTimeBucketAssigner<>());

		stream.addSink(new TestFileSink<>(builder, 1000));

		env.execute();
//		validateResults(folder, SpecificData.get(), data);
	}

	private static class TestHadoopPathBasedBulkWriterFactory extends HadoopPathBasedBulkWriterFactory<String> {

		@Override
		public PathBasedBulkWriter<String> create(Path targetPath, Path inProgressPath) {
			try {
				FileSystem fileSystem = FileSystem.get(inProgressPath.toUri(), new Configuration());
				FSDataOutputStream output = fileSystem.create(inProgressPath);
				return new FSDataOutputStreamBulkWriter(output);
			} catch (IOException e) {
				ExceptionUtils.rethrow(e);
			}

			return null;
		}
	}

	private static class FSDataOutputStreamBulkWriter implements PathBasedBulkWriter<String> {
		private final FSDataOutputStream outputStream;

		public FSDataOutputStreamBulkWriter(FSDataOutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public void dispose() {
			try {
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void addElement(String element) throws IOException {
			outputStream.writeBytes(element + "\n");
		}

		@Override
		public void flush() throws IOException {
			outputStream.flush();
		}

		@Override
		public void finish() throws IOException {
			outputStream.flush();
			outputStream.close();
		}
	}
}
