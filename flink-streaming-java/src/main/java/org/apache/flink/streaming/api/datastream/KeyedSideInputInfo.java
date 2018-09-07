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
import org.apache.flink.streaming.api.functions.KeyedSideInputProcessFunction;
import org.apache.flink.util.Preconditions;

/**
 * Javadoc.
 */
public class KeyedSideInputInfo<I, K, O> extends SideInputInfo<I> {

	private final KeyedSideInputProcessFunction<I, K, O> function;

	private final KeySelector<I, K> keySelector;

	private final TypeInformation<K> keyTypeInfo;

	public KeyedSideInputInfo(
			final KeyedStream<I, K> stream,
			final KeyedSideInputProcessFunction<I, K, O> function
	) {
		super(stream);
		this.function = Preconditions.checkNotNull(function);
		this.keySelector = stream.getKeySelector();
		this.keyTypeInfo = stream.getKeyType();
	}

	public KeyedSideInputProcessFunction<I, K, O> getFunction() {
		return function;
	}

	public KeySelector<I, K> getKeySelector() {
		return keySelector;
	}

	public TypeInformation<K> getKeyTypeInfo() {
		return keyTypeInfo;
	}
}
