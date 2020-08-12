package org.apache.flink.api.connector.sink;

import org.apache.flink.metrics.MetricGroup;

import java.util.List;

public interface WriterInitContext<CommT> {

	/**
	 * @return The metric group this source belongs to.
	 */
	MetricGroup metricGroup();

	// Adds committables upon restore
	void commit(List<CommT> committables);

	void sendEventToCoordinator(SinkEvent event);
}
