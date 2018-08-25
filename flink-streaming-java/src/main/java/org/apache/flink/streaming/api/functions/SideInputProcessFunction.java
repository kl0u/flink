package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public abstract class SideInputProcessFunction<IN, OUT> extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	// TODO: 8/29/18 also expose the TAG
	public abstract void process(IN value, Collector<OUT> out) throws Exception;
}
