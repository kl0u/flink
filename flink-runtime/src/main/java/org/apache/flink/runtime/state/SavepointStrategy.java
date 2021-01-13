package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import javax.annotation.Nonnull;

import java.util.concurrent.RunnableFuture;

@Internal
public interface SavepointStrategy<S extends StateObject> {

    @Nonnull
    RunnableFuture<S> takeSavepoint(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception;
}
