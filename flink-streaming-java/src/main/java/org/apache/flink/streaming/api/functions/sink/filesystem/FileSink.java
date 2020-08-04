package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FileSink<BucketID, IN> implements Sink<BucketID, IN, InProgressFileWriter.PendingFileRecoverable, FileWriterState<BucketID>, Long> {

	private final BucketsBuilder<IN, BucketID, ? extends BucketsBuilder<IN, BucketID, ?>> bucketsBuilder;

	private final long bucketCheckInterval;

	private FileSink(
			final BucketsBuilder<IN, BucketID, ? extends BucketsBuilder<IN, BucketID, ?>> bucketsBuilder,
			final long bucketCheckInterval) {
		this.bucketsBuilder = checkNotNull(bucketsBuilder);
		this.bucketCheckInterval = bucketCheckInterval;
	}

	@Override
	public FileWriter<BucketID, IN> getWriter(final WriterContext<BucketID> context) throws IOException {
		checkNotNull(context);
		return bucketsBuilder.createWriter(context);
	}

	@Override
	public Committer<InProgressFileWriter.PendingFileRecoverable> getCommitter() throws IOException {
		return bucketsBuilder.createCommitter();
	}

	public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(
			final Path basePath, final Encoder<IN> encoder) {
		return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
	}

	@Internal
	public abstract static class BucketsBuilder<IN, BucketID, T extends BucketsBuilder<IN, BucketID, T>> implements Serializable {

		private static final long serialVersionUID = 1L;

		public static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

		@SuppressWarnings("unchecked")
		protected T self() {
			return (T) this;
		}

		@Internal
		public abstract FileWriter<BucketID, IN> createWriter(final WriterContext<BucketID> context) throws IOException;

		@Internal
		public abstract FileCommitter createCommitter() throws IOException;
	}


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
		public FileWriter<BucketID, IN> createWriter(final WriterContext<BucketID> context) throws IOException {
			return new FileWriter<>(
					context.getSubtaskId(),
					context.getWriterID(),
					bucketAssigner.getSerializer(),
					basePath,
					new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder),
					rollingPolicy,
					outputFileConfig);
		}

		@Internal
		@Override
		public FileCommitter createCommitter() throws IOException {
			final BucketWriter<IN, BucketID> committer =
					new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
			return new FileCommitter(committer);
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
