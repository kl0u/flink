package org.apache.flink.streaming.api.functions.sink.filesystem;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class WriterState<WriterID> {

	private final WriterID writerID;

	public WriterState(WriterID writerID) {
		this.writerID = checkNotNull(writerID);
	}

	public WriterID getWriterID() {
		return writerID;
	}
}
