package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import javax.annotation.Nullable;

import java.io.Serializable;

public interface WriterAssigner<IN, WriterID> extends Serializable {

	/**
	 * Returns the identifier of the bucket the provided element should be put into.
	 * @param element The current element being processed.
	 * @param context The {@link SinkFunction.Context context} used by the {@link StreamingFileSink sink}.
	 *
	 * @return A string representing the identifier of the bucket the element should be put into.
	 * The actual path to the bucket will result from the concatenation of the returned string
	 * and the {@code base path} provided during the initialization of the {@link StreamingFileSink sink}.
	 */
	WriterID getWriterId(IN element, Context context);

	/**
	 * @return A {@link SimpleVersionedSerializer} capable of serializing/deserializing the elements
	 * of type {@code BucketID}. That is the type of the objects returned by the
	 * {@link #getWriterId(Object, Context)}.
	 */
	SimpleVersionedSerializer<WriterID> getSerializer();

	/**
	 * Context that the {@link BucketAssigner} can use for getting additional data about
	 * an input record.
	 *
	 * <p>The context is only valid for the duration of a {@link BucketAssigner#getBucketId(Object, BucketAssigner.Context)} call.
	 * Do not store the context and use afterwards!
	 */
	@PublicEvolving
	interface Context {

		/**
		 * Returns the current processing time.
		 */
		long currentProcessingTime();

		/**
		 * Returns the current event-time watermark.
		 */
		long currentWatermark();

		/**
		 * Returns the timestamp of the current input record or
		 * {@code null} if the element does not have an assigned timestamp.
		 */
		@Nullable
		Long timestamp();
	}
}
