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
import org.apache.flink.streaming.api.datastream.SideInputInfo;
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

	private final Map<InputTag, SideInputInfo<?, ?, OUT>> inputInfo;

	public MultiInputTransformation(
			final String name,
			final MultiInputStreamOperator<OUT> operator,
			final Map<InputTag, SideInputInfo<?, ?, OUT>> sideInputInfo,
			final TypeInformation<OUT> outputType,
			final int parallelism
	) {
		super(name, outputType, parallelism);
		Preconditions.checkState(sideInputInfo != null && sideInputInfo.size() >= 1); // it has to have at least the main input

		this.operator = Preconditions.checkNotNull(operator);
		this.inputInfo = sideInputInfo;
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

	public Map<InputTag, SideInputInfo<?, ?, OUT>> getInputInfo() {
		return Collections.unmodifiableMap(inputInfo);
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		final Collection<StreamTransformation<?>> result = new ArrayList<>();
		result.add(this);
		for (SideInputInfo<?, ?, OUT> input: inputInfo.values()) {
			result.addAll(input.getTransformation().getTransitivePredecessors());
		}
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}
}
