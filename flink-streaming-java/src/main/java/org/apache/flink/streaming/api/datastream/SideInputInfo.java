package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.SideInputProcessFunction;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

public class SideInputInfo<T, K, O> {

	private final DataStream<T> stream;

	private final SideInputProcessFunction<T, O> function;

	@Nullable
	private final KeySelector<T, K> keySelector;

	@Nullable
	private final TypeInformation<K> keyTypeInfo;

	public SideInputInfo(
			final DataStream<T> stream,
			final SideInputProcessFunction<T, O> function
	) {
		this.stream = Preconditions.checkNotNull(stream);
		this.function = Preconditions.checkNotNull(function);

		if (stream instanceof KeyedStream) {
			final KeyedStream<T, K> keyedSideInput = (KeyedStream<T, K>) stream;
			this.keySelector = keyedSideInput.getKeySelector();
			this.keyTypeInfo = keyedSideInput.getKeyType();
		} else {
			this.keySelector = null;
			this.keyTypeInfo = null;
		}
	}

	public StreamTransformation<T> getTransformation() {
		return stream.getTransformation();
	}

	public TypeInformation<T> getInputTypeInfo() {
		return stream.getType();
	}

	public SideInputProcessFunction<T, O> getFunction() {
		return function;
	}

	@Nullable
	public KeySelector<T, K> getKeySelector() {
		return keySelector;
	}

	@Nullable
	public TypeInformation<K> getKeyTypeInfo() {
		return keyTypeInfo;
	}
}