/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.NInputStreamOperator;
import org.apache.flink.streaming.runtime.io.InputProcessor;

/**
 * Javadoc.
 */
public class NInputStreamTask<IN, OUT> extends StreamTask<OUT, NInputStreamOperator<IN, OUT>> {

	// TODO: 8/24/18 also add the metrics
	private InputProcessor inputProcessor;

	private volatile boolean running = true;

	public NInputStreamTask(Environment env) {
		super(env);
	}

	@Override
	protected void init() throws Exception {
		throw new UnsupportedOperationException();
		// TODO: 8/24/18 implement this.
	}

	@Override
	protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final InputProcessor processor = inputProcessor;

		while (running && processor.processInput()) {
			// all the work happens in the "processInput" method
		}
	}

	@Override
	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() throws Exception {
		running = false;
	}
}
