package org.apache.flink.streaming.api.functions.sink.filesystem;

public class WriterProperties {

	boolean requiresCleanupOfInProgressFileRecoverableState();

	boolean supportsResume();

}
