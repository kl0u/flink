package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public  interface Sink<WriterID, IN, OUT extends Committable, WriterStateT extends WriterState<WriterID>, SharedStateT> {

	Writer<WriterID, IN, OUT, WriterStateT, SharedStateT> getWriter(WriterContext<WriterID> context) throws Exception;

	Committer<OUT> getCommitter() throws Exception;

	SimpleVersionedSerializer<OUT> getCommittableSerializer() throws Exception;
}
