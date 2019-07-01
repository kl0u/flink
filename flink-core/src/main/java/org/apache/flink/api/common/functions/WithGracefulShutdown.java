/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
