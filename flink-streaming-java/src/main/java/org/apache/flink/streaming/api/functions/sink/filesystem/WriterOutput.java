package org.apache.flink.streaming.api.functions.sink.filesystem;

public interface WriterOutput<OUT extends Committable> {

	void collect(OUT element);
}
