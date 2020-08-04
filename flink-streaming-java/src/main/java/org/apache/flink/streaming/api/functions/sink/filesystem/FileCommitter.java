package org.apache.flink.streaming.api.functions.sink.filesystem;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * In Pipelined mode, the operator will do the following:
 * - onElement: store the received split in a list
 * - onCheckpoint: put the list in a Map with the checkpointID as key and checkpoint the map
 * - onCheckpointComplete: commit up to that checkpoint
 */
public class FileCommitter implements Committer<InProgressFileWriter.PendingFileRecoverable> {

	private final CommitFileRecoverable commiter;

	public FileCommitter(final CommitFileRecoverable committer) {
		this.commiter = checkNotNull(committer);
	}

	@Override
	public void commit(final InProgressFileWriter.PendingFileRecoverable committable) throws IOException {
		checkNotNull(committable);
		commiter.recoverPendingFile(committable).commit(); // TODO: 04.08.20 or commitAfterRecovery() we will see.
	}
}
