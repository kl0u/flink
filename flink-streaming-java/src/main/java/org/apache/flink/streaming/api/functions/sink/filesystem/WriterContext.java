package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.metrics.MetricGroup;

public interface WriterContext<WriterID> {

	int getSubtaskId();

	WriterID getWriterID();

	long getWriterCheckInterval();

	/**
	 * @return The metric group this source belongs to.
	 */
	MetricGroup metricGroup();
}
