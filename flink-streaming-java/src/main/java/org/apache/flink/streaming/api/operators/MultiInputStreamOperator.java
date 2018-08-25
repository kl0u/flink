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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.functions.SideInputProcessFunction;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Javadoc.
 * todo this will become the new new AbstractStreamOperator
 */
public class MultiInputStreamOperator<OUT> extends AbstractStreamOperator<OUT> implements StreamOperator<OUT>, Serializable {

	private static final long serialVersionUID = 1L;

	/** The logger used by the operator class and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(MultiInputStreamOperator.class);

	private Map<InputTag, SideInputProcessFunction<?, OUT>> sideInputFunctions;

	// ---------------- runtime fields ------------------

	private transient Map<InputTag, SideInputOperator<?, OUT>> sideInputOperators;

	// TODO: 8/29/18 here it could be directly operator instead of function
	public MultiInputStreamOperator(
			final Map<InputTag, SideInputProcessFunction<?, OUT>> sideInputFunctions
	) {
		this.sideInputFunctions = Preconditions.checkNotNull(sideInputFunctions);
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.sideInputOperators = new HashMap<>();
		for (Map.Entry<InputTag, SideInputProcessFunction<?, OUT>> e : sideInputFunctions.entrySet()) {
			final SideInputOperator<?, OUT> operator = new SideInputOperator<>(this, output, e.getValue());
			sideInputOperators.put(e.getKey(), operator);
			operator.open();
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		for (Map.Entry<InputTag, SideInputOperator<?, OUT>> e : sideInputOperators.entrySet()) {
			// use tag for logging
			e.getValue().close();
		}
	}

	public SideInputOperator<?, OUT> getTagOperator(final InputTag tag) {
		final SideInputOperator<?, OUT> operator = sideInputOperators.get(tag);
		Preconditions.checkState(operator != null, "Unknown tag: " + tag);
		return operator;
	}
}
