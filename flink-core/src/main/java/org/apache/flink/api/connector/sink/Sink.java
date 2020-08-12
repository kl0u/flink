package org.apache.flink.api.connector.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface Sink<IN, CommT, WriterState> extends Serializable {
	Writer<IN, CommT, WriterState> createWriter(WriterInitContext<CommT> context) throws Exception;

	// TODO how to create committables out of this method. It might be necessary to commit some parts of the state, e.g.
	// keep a single transaction open and commit all other
	Writer<IN, CommT, WriterState> restoreWriter(
		WriterInitContext<CommT> context,
		List<WriterState> checkpoint) throws Exception;

	Optional<SinkCoordinator> getCoordinator();

	Committer<CommT> createCommitter() throws Exception;

	SimpleVersionedSerializer<CommT> getCommittableSerializer() throws Exception;

	SimpleVersionedSerializer<WriterState> getWriterStateSerializer() throws Exception;

	interface SinkCoordinator {
		void handleEventFromSink(int subtaskId, SinkEvent event);
	}
}
