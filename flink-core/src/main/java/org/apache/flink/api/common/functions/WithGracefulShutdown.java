package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface to be implemented by functions that want to perform
 * specific actions when the End-Of-Stream is reached.
 *
 * <p>An example is any "two-phase-commit" sink which in order to
 * guarantee exactly-once end-to-end semantics, they park data in
 * temporary buffers and commit them when it is guaranteed that
 * this data will never be invalidated due to a rewind in case of a
 * failure. In this case, when the end of the stream is reached, such
 * a sink may need to close any in-progress buffers and commit the
 * contained data.
 */
@PublicEvolving
public interface WithGracefulShutdown {

	default void prepareToShutdown() throws Exception {

	}

	default void shutdown() throws Exception {

	}
}
