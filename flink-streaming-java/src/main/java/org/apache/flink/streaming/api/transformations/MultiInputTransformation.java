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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.KeyedSideInputInfo;
import org.apache.flink.streaming.api.datastream.NonKeyedSideInputInfo;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.MultiInputStreamOperator;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Javadoc.
 */
public class MultiInputTransformation<OUT> extends StreamTransformation<OUT> {

	private final MultiInputStreamOperator<OUT> operator;

	private final Map<InputTag, NonKeyedSideInputInfo<?, OUT>> nonKeyedInputInfo;

	private final Map<InputTag, KeyedSideInputInfo<?, ?, OUT>> keyedInputInfo;

	public MultiInputTransformation(
			final String name,
			final MultiInputStreamOperator<OUT> operator,
			final Map<InputTag, NonKeyedSideInputInfo<?, OUT>> nonKeyedInputInfo,
			final Map<InputTag, KeyedSideInputInfo<?, ?, OUT>> keyedInputInfo,
			final TypeInformation<OUT> outputType,
			final int parallelism
	) {
		super(name, outputType, parallelism);

		this.nonKeyedInputInfo = Preconditions.checkNotNull(nonKeyedInputInfo);
		this.keyedInputInfo = Preconditions.checkNotNull(keyedInputInfo);
		this.operator = Preconditions.checkNotNull(operator);

		// there must be at least one input stream (the main one)
		Preconditions.checkState(nonKeyedInputInfo.size() + keyedInputInfo.size() >= 1);
	}

	/**
	 * Returns the {@code TypeInformation} for the elements of the input.
	 */
	public TypeInformation<?> getInputType() {
		return outputType;
	}

	/**
	 * Returns the {@code NInputStreamOperator} of this Transformation.
	 */
	public MultiInputStreamOperator<OUT> getOperator() {
		return operator;
	}

	public Map<InputTag, NonKeyedSideInputInfo<?, OUT>> getNonKeyedInputInfo() {
		return Collections.unmodifiableMap(nonKeyedInputInfo);
	}

	public Map<InputTag, KeyedSideInputInfo<?, ?, OUT>> getKeyedInputInfo() {
		return Collections.unmodifiableMap(keyedInputInfo);
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		final Collection<StreamTransformation<?>> result = new ArrayList<>();

		// TODO: 9/10/18 is this correct??? should we add ourselves???

		// we first add ourselves
		result.add(this);

		// and then we go on to add the non-keyed inputs
		for (NonKeyedSideInputInfo<?, OUT> input: nonKeyedInputInfo.values()) {
			result.addAll(input.getTransformation().getTransitivePredecessors());
		}

		// and finally the keyed inputs
		for (KeyedSideInputInfo<?, ?, OUT> input: keyedInputInfo.values()) {
			result.addAll(input.getTransformation().getTransitivePredecessors());
		}
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}
}
