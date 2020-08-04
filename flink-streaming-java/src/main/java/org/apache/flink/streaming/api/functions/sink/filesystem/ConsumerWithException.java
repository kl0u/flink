package org.apache.flink.streaming.api.functions.sink.filesystem;

@FunctionalInterface
public interface ConsumerWithException<T, E extends Throwable> {

	void accept(T element) throws E;
}
