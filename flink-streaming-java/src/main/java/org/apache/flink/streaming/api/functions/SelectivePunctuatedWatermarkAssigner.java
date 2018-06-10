package org.apache.flink.streaming.api.functions;

public interface SelectivePunctuatedWatermarkAssigner<IN> extends AssignerWithPunctuatedWatermarks<IN> {

	String getTag();

	boolean select(IN element);
}
