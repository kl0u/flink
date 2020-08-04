package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class BatchCommitterOperator<IN extends Committable>
		extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<IN, Void> {

	private final Committer<IN> committer;

	public BatchCommitterOperator(final Committer<IN> committer) {
		this.committer = checkNotNull(committer);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		committer.commit(element.getValue());
	}
}
