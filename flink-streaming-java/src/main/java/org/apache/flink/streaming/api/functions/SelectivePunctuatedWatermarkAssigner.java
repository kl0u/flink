package org.apache.flink.streaming.api.functions;

public interface SelectivePunctuatedWatermarkAssigner<IN, KEY> extends AssignerWithPunctuatedWatermarks<IN> {

	String getTag();

	boolean select(KEY key);
}
