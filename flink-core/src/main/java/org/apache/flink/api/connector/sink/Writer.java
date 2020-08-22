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

package org.apache.flink.api.connector.sink;

import java.io.IOException;
import java.util.List;

/**
 * Javadoc.
 */
public interface Writer<IN, CommT, StateT, SharedStateT> {

	void init(List<StateT> subtaskState, List<SharedStateT> sharedState, WriterOutput<CommT> output) throws Exception;

	void write(IN element, Context<CommT> ctx, WriterOutput<CommT> output) throws Exception;

	List<StateT> snapshotState(WriterOutput<CommT> output) throws Exception;

	void flush(WriterOutput<CommT> output) throws Exception;

	SharedStateT snapshotSharedState() throws Exception;

	/**
	 * Javadoc.
	 */
	interface Context<CommT> {

		long currentProcessingTime();

		long currentWatermark();

		Long timestamp();

		void registerCallback(TimerCallback<CommT> callback, long delay);
	}

	/**
	 * Javadoc.
	 */
	interface TimerCallback<CommT> {
		void apply(Context<CommT> ctx, WriterOutput<CommT> output) throws IOException;
	}
}
