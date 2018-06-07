package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

public interface SelectiveWatermarkAssigner<IN> extends Function, Serializable {

	String getId();

	boolean select(IN field);

	long extractTimestamp(IN element, long previousElementTimestamp);

	// TODO: 6/7/18 boxed to keep the null semantics with the old assigner
	Long getCurrentWatermark();
}
