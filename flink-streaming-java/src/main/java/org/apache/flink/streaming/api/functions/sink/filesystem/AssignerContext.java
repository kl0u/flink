package org.apache.flink.streaming.api.functions.sink.filesystem;

import javax.annotation.Nullable;

public class AssignerContext implements WriterAssigner.Context {

	@Nullable
	private Long elementTimestamp;

	private long currentWatermark;

	private long currentProcessingTime;

	AssignerContext() {
		this.elementTimestamp = null;
		this.currentWatermark = Long.MIN_VALUE;
		this.currentProcessingTime = Long.MIN_VALUE;
	}

	void update(@Nullable Long elementTimestamp, long watermark, long processingTime) {
		this.elementTimestamp = elementTimestamp;
		this.currentWatermark = watermark;
		this.currentProcessingTime = processingTime;
	}

	@Override
	public long currentProcessingTime() {
		return currentProcessingTime;
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	@Nullable
	public Long timestamp() {
		return elementTimestamp;
	}
}
