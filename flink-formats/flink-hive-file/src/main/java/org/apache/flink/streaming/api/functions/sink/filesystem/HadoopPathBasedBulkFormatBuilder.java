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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.hive.DefaultHadoopFileCommitterFactory;
import org.apache.flink.formats.hive.HadoopFileCommitterFactory;
import org.apache.flink.formats.hive.HadoopPathBasedBulkWriterFactory;
import org.apache.flink.formats.hive.HadoopPathBasedPartFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 *
 */
public class HadoopPathBasedBulkFormatBuilder<IN, BucketID, T extends HadoopPathBasedBulkFormatBuilder<IN, BucketID, T>>
	extends StreamingFileSink.BucketsBuilder<IN, BucketID, T> {

	private static final long serialVersionUID = 1L;

	private final Path basePath;

	private long bucketCheckInterval;

	private HadoopPathBasedBulkWriterFactory<IN> writerFactory;

	private HadoopFileCommitterFactory fileCommitterFactory;

	private SerializableConfiguration serializableConfiguration;

	private BucketAssigner<IN, BucketID> bucketAssigner;

	private CheckpointRollingPolicy<IN, BucketID> rollingPolicy;

	private BucketFactory<IN, BucketID> bucketFactory;

	private OutputFileConfig outputFileConfig;

	public HadoopPathBasedBulkFormatBuilder(
		org.apache.hadoop.fs.Path basePath,
		HadoopPathBasedBulkWriterFactory<IN> writerFactory,
		Configuration configuration,
		BucketAssigner<IN, BucketID> assigner) {

		this(
			basePath,
			writerFactory,
			new DefaultHadoopFileCommitterFactory(),
			configuration,
			assigner,
			OnCheckpointRollingPolicy.build(),
			DEFAULT_BUCKET_CHECK_INTERVAL,
			new DefaultBucketFactoryImpl<>(),
			OutputFileConfig.builder().build());
	}

	public HadoopPathBasedBulkFormatBuilder(
		org.apache.hadoop.fs.Path basePath,
		HadoopPathBasedBulkWriterFactory<IN> writerFactory,
		HadoopFileCommitterFactory fileCommitterFactory,
		Configuration configuration,
		BucketAssigner<IN, BucketID> assigner,
		CheckpointRollingPolicy<IN, BucketID> policy,
		long bucketCheckInterval,
		BucketFactory<IN, BucketID> bucketFactory,
		OutputFileConfig outputFileConfig) {

		this.basePath = new Path(Preconditions.checkNotNull(basePath).toString());
		this.writerFactory = writerFactory;
		this.fileCommitterFactory = fileCommitterFactory;
		this.serializableConfiguration = new SerializableConfiguration(configuration);
		this.bucketAssigner = Preconditions.checkNotNull(assigner);
		this.rollingPolicy = Preconditions.checkNotNull(policy);
		this.bucketCheckInterval = bucketCheckInterval;
		this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
		this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
	}

	public T withBucketCheckInterval(long interval) {
		this.bucketCheckInterval = interval;
		return self();
	}

	public T withBucketAssigner(BucketAssigner<IN, BucketID> assigner) {
		this.bucketAssigner = Preconditions.checkNotNull(assigner);
		return self();
	}

	public T withRollingPolicy(CheckpointRollingPolicy<IN, BucketID> rollingPolicy) {
		this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
		return self();
	}

	public T withBucketFactory(BucketFactory<IN, BucketID> factory) {
		this.bucketFactory = Preconditions.checkNotNull(factory);
		return self();
	}

	public T withOutputFileConfig(OutputFileConfig outputFileConfig) {
		this.outputFileConfig = outputFileConfig;
		return self();
	}

	public T withConfiguration(Configuration configuration) {
		this.serializableConfiguration = new SerializableConfiguration(configuration);
		return self();
	}

	@Override
	public Buckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
		return new Buckets<>(
			basePath,
			bucketAssigner,
			bucketFactory,
			new HadoopPathBasedPartFileWriter.Factory<>(
				serializableConfiguration.getConfiguration(),
				writerFactory,
				fileCommitterFactory),
			rollingPolicy,
			subtaskIndex,
			outputFileConfig);
	}
}
