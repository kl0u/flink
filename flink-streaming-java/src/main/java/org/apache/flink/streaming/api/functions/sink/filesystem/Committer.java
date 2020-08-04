package org.apache.flink.streaming.api.functions.sink.filesystem;

import java.io.IOException;

/**
 * todo this will be stateful so we will need some methods like getState and getStateSerializer.
 * or this can be done through a context where the user can registerState and registerUnionState
 */
public interface Committer<IN extends Committable> {

	void commit(IN committable) throws IOException;
}
