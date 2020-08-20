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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.InitContext;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
public class FileSink<BucketID, IN> implements Sink<IN, InProgressFileWriter.PendingFileRecoverable, FileWriterState<BucketID>> {

	private final BucketsBuilder<IN, BucketID, ? extends BucketsBuilder<IN, BucketID, ?>> bucketsBuilder;

	private final long bucketCheckInterval;

	private FileSink(
			final BucketsBuilder<IN, BucketID, ? extends BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
			final long bucketCheckInterval) {
		this.bucketsBuilder = checkNotNull(bucketsBuilder);
		this.bucketCheckInterval = bucketCheckInterval;
	}

	@Override
	public FileWriterTracker<IN, BucketID> createWriter(final InitContext context) throws IOException {
		checkNotNull(context);
		return bucketsBuilder.createWriter(context);
	}

	@Override
	public Committer<InProgressFileWriter.PendingFileRecoverable> createCommitter() throws IOException {
		return bucketsBuilder.createCommitter();
	}

	@Override
	public SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> getCommittableSerializer() throws Exception {
		return bucketsBuilder.getCommittableSerializer();
	}

	@Override
	public SimpleVersionedSerializer<FileWriterState<BucketID>> getStateSerializer() throws IOException {
		return bucketsBuilder.getWriterStateSerializer();
	}

	public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(
			final Path basePath, final Encoder<IN> encoder) {
		return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
	}

	/**
	 * Javadoc.
	 */
	@Internal
	public abstract static class BucketsBuilder<IN, BucketID, T extends BucketsBuilder<IN, BucketID, T>> implements Serializable {

		private static final long serialVersionUID = 1L;

		public static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

		@SuppressWarnings("unchecked")
		protected T self() {
			return (T) this;
		}

		@Internal
		public abstract FileWriterTracker<IN, BucketID> createWriter(final InitContext context) throws IOException;

		@Internal
		public abstract FileCommitter createCommitter() throws IOException;

		public abstract SimpleVersionedSerializer<FileWriterState<BucketID>> getWriterStateSerializer() throws IOException;

		public abstract SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> getCommittableSerializer() throws IOException;
	}

	/**
	 * Javadoc.
	 */
	public static class RowFormatBuilder<IN, BucketID, T extends RowFormatBuilder<IN, BucketID, T>> extends BucketsBuilder<IN, BucketID, T> {

		private static final long serialVersionUID = 1L;

		private long bucketCheckInterval;

		private final Path basePath;

		private Encoder<IN> encoder;

		private BucketAssigner<IN, BucketID> bucketAssigner;

		private RollingPolicy<IN, BucketID> rollingPolicy;

		private BucketFactory<IN, BucketID> bucketFactory;

		private OutputFileConfig outputFileConfig;

		protected RowFormatBuilder(Path basePath, Encoder<IN> encoder, BucketAssigner<IN, BucketID> bucketAssigner) {
			this(basePath, encoder, bucketAssigner, DefaultRollingPolicy.builder().build(), DEFAULT_BUCKET_CHECK_INTERVAL, new DefaultBucketFactoryImpl<>(), OutputFileConfig.builder().build());
		}

		protected RowFormatBuilder(
				Path basePath,
				Encoder<IN> encoder,
				BucketAssigner<IN, BucketID> assigner,
				RollingPolicy<IN, BucketID> policy,
				long bucketCheckInterval,
				BucketFactory<IN, BucketID> bucketFactory,
				OutputFileConfig outputFileConfig) {
			this.basePath = checkNotNull(basePath);
			this.encoder = checkNotNull(encoder);
			this.bucketAssigner = checkNotNull(assigner);
			this.rollingPolicy = checkNotNull(policy);
			this.bucketCheckInterval = bucketCheckInterval;
			this.bucketFactory = checkNotNull(bucketFactory);
			this.outputFileConfig = checkNotNull(outputFileConfig);
		}

		public long getBucketCheckInterval() {
			return bucketCheckInterval;
		}

		public T withBucketCheckInterval(final long interval) {
			this.bucketCheckInterval = interval;
			return self();
		}

		public T withBucketAssigner(final BucketAssigner<IN, BucketID> assigner) {
			this.bucketAssigner = checkNotNull(assigner);
			return self();
		}

		public T withRollingPolicy(final RollingPolicy<IN, BucketID> policy) {
			this.rollingPolicy = checkNotNull(policy);
			return self();
		}

		public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
			this.outputFileConfig = outputFileConfig;
			return self();
		}

		public <ID> RowFormatBuilder<IN, ID, ? extends RowFormatBuilder<IN, ID, ?>> withNewBucketAssignerAndPolicy(final BucketAssigner<IN, ID> assigner, final RollingPolicy<IN, ID> policy) {
			Preconditions.checkState(bucketFactory.getClass() == DefaultBucketFactoryImpl.class, "newBuilderWithBucketAssignerAndPolicy() cannot be called after specifying a customized bucket factory");
			return new RowFormatBuilder(basePath, encoder, checkNotNull(assigner), checkNotNull(policy), bucketCheckInterval, new DefaultBucketFactoryImpl<>(), outputFileConfig);
		}

		/** Creates the actual sink. */
		public FileSink<BucketID, IN> build() {
			return new FileSink<>(this, bucketCheckInterval);
		}

		@VisibleForTesting
		T withBucketFactory(final BucketFactory<IN, BucketID> factory) {
			this.bucketFactory = checkNotNull(factory);
			return self();
		}

		@Internal
		@Override
		public FileWriterTracker<IN, BucketID> createWriter(final InitContext context) throws IOException {
			return new FileWriterTracker<>(
					context.getSubtaskId(),
					context.getAttemptID(),
					basePath,
					bucketAssigner,
					new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder),
					rollingPolicy,
					outputFileConfig,
					bucketCheckInterval);
		}

		@Internal
		@Override
		public FileCommitter createCommitter() throws IOException {
			final BucketWriter<IN, BucketID> committer =
					new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
			return new FileCommitter(committer);
		}

		@Override
		public SimpleVersionedSerializer<FileWriterState<BucketID>> getWriterStateSerializer() throws IOException {
			final SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> serializer =
					new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder)
							.getProperties()
							.getInProgressFileRecoverableSerializer();
			return new FileWriterStateSerializer<>(bucketAssigner.getSerializer(), serializer);
		}

		@Override
		public SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> getCommittableSerializer() throws IOException {
			return new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder)
					.getProperties()
					.getPendingFileRecoverableSerializer();
		}
	}

	/**
	 * Builder for the vanilla {@link StreamingFileSink} using a row format.
	 * @param <IN> record type
	 */
	public static final class DefaultRowFormatBuilder<IN> extends RowFormatBuilder<IN, String, DefaultRowFormatBuilder<IN>> {
		private static final long serialVersionUID = -8503344257202146718L;

		private DefaultRowFormatBuilder(Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
			super(basePath, encoder, bucketAssigner);
		}
	}
}
