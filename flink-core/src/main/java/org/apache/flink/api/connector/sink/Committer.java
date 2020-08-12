package org.apache.flink.api.connector.sink;

import java.util.List;

/**
 * A committer that
 */
public interface Committer<IN> {
	void commit(List<IN> committable) throws Exception;

	// 12-08-2020: a method to clear committables, e.g. when a task sent the committables twice
	// it would be a best-effort cleaning up
	void cleanUp(List<IN> committables) throws Exception;
}

//SourceCoordinator {
//
//	HashMap<subTaskId, List<CommT>>
//
//		abort(oldCommtiables)
//}
