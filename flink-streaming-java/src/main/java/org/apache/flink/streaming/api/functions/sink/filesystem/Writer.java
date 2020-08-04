package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

import java.io.IOException;
import java.util.List;

public interface Writer<WriterID, IN, OUT extends Committable, State extends WriterState<WriterID>, SharedState> {

	boolean isActive();

	void initState(State subtaskState, List<SharedState> commonState, WriterOutput<OUT> output) throws Exception;

	void combine(Writer<WriterID, IN, OUT, State, SharedState> writer, WriterOutput<OUT> output) throws Exception;

	void write(IN element, Context ctx, WriterOutput<OUT> output) throws Exception;

	default void onPeriodicCheck(long currentTime, WriterOutput<OUT> output) throws Exception {}

	// TODO: 05.08.20 if we have an iD in the state, then the return here should be sth like simple SimpleVersionedSerializerWithID or sth
	// this will simplify the operator.
	// TODO: 10.08.20 these methods may need to move to the sink
	SimpleVersionedSerializer<State> getSubtaskStateSerializer();

	State getSubtaskState(WriterOutput<OUT> output) throws Exception;

	SimpleVersionedSerializer<SharedState> getCommonStateSerializer();

	SharedState getCommonState() throws Exception;

	interface Context {

		long currentProcessingTime();

		long currentWatermark();

		Long timestamp();
	}
}
