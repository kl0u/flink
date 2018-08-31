/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.SideInputProcessFunction;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Javadoc.
 */
public class SideInputInfo<T, K, O> {

	private final DataStream<T> stream;

	private final SideInputProcessFunction<T, O> function;

	@Nullable
	private final KeySelector<T, K> keySelector;

	@Nullable
	private final TypeInformation<K> keyTypeInfo;

	public SideInputInfo(
			final DataStream<T> stream,
			final SideInputProcessFunction<T, O> function
	) {
		this.stream = Preconditions.checkNotNull(stream);
		this.function = Preconditions.checkNotNull(function);

		if (stream instanceof KeyedStream) {
			final KeyedStream<T, K> keyedSideInput = (KeyedStream<T, K>) stream;
			this.keySelector = keyedSideInput.getKeySelector();
			this.keyTypeInfo = keyedSideInput.getKeyType();
		} else {
			this.keySelector = null;
			this.keyTypeInfo = null;
		}
	}

	public StreamTransformation<T> getTransformation() {
		return stream.getTransformation();
	}

	public TypeInformation<T> getInputTypeInfo() {
		return stream.getType();
	}

	public SideInputProcessFunction<T, O> getFunction() {
		return function;
	}

	@Nullable
	public KeySelector<T, K> getKeySelector() {
		return keySelector;
	}

	@Nullable
	public TypeInformation<K> getKeyTypeInfo() {
		return keyTypeInfo;
	}
}
