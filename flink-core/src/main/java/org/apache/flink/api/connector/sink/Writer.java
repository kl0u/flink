package org.apache.flink.api.connector.sink;

import java.util.List;

public interface Writer<IN, CommT, StateT> {
	void write(IN element, Context<CommT> ctx, WriterOutput<CommT> output) throws Exception;

	// STREAM: Must drain.
	void persist(WriterOutput<CommT> output) throws Exception;

	List<StateT> checkpoint(WriterOutput<CommT> output);

	// best-effort cleaning. Called on a failure
	void cleanUp();

	void handleEventFromCoordinator(SinkEvent event);

	interface WriterOutput<CommT> {
		void commit(CommT commitable);

		void sendToSinkCoordinator(SinkEvent event);
	}

	interface Context<CommT> {
		void registerCallback(Callback<CommT> callback, long delay);
	}

	interface Callback<CommT> {
		void apply(Context<CommT> ctx, WriterOutput<CommT> output);
	}
}
