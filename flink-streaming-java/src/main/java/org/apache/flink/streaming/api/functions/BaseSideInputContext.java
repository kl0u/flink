/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * The base context available to all methods in process function.
 */
@PublicEvolving
public interface BaseSideInputContext {

	/**
	 * Timestamp of the element currently being processed or timestamp of a firing timer.
	 *
	 * <p>This might be {@code null}, for example if the time characteristic of your program
	 * is set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
	 */
	@Nullable
	Long timestamp();

	/**
	 * Emits a record to the side output identified by the {@link OutputTag}.
	 *
	 * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
	 * @param value The record to emit.
	 */
	<X> void output(final OutputTag<X> outputTag, final X value);

	/** Returns the current processing time. */
	long currentProcessingTime();

	/** Returns the current event-time watermark. */
	long currentWatermark();
}
