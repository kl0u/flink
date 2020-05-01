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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

/**
 * Specialized
 */
public interface PathBasedBulkWriter<T> extends BulkWriter<T> {

	void dispose();

	// ------------------------------------------------------------------------

	/**
	 * A factory that creates a {@link PathBasedBulkWriter}.
	 *
	 * @param <T> The type of record to write.
	 */
	@FunctionalInterface
	public interface Factory<T> extends Serializable {

		PathBasedBulkWriter<T> create(Path targetPath, Path inProgressPath);

	}
}
