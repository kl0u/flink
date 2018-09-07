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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

/**
 * Javadoc.
 */
public abstract class KeyedSideInputProcessFunction<I, K, O> extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	public abstract void processElement(final I value, final Context<K> ctx, final Collector<O> out) throws Exception;

	/**
	 * Called when a timer set using {@link TimerService} fires.
	 *
	 * @param timestamp The timestamp of the firing timer.
	 * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link TimeDomain}, and the key
	 *            of the firing timer and getting a {@link TimerService} for registering timers and querying the time.
	 *            The context is only valid during the invocation of this method, do not store it.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	public void onTimer(final long timestamp, OnTimerContext<K> ctx, Collector<O> out) throws Exception {}

	/**
	 * Javadoc.
	 */
	@PublicEvolving
	public interface Context<K> extends BaseSideInputContext {

		/**
		 * Get key of the element being processed.
		 */
		K getCurrentKey();

		/**
		 * A {@link TimerService} for querying time and registering timers.
		 */
		TimerService timerService();

		// TODO: 9/9/18 I would also like to not give access to the runtime context but have special methods for state.
		// also state could be pre-declared as in the Broadcast state and when you access it, there is a check if you
		// have declared it.
	}

	/**
	 * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
	 */
	public interface OnTimerContext<K> extends Context<K> {

		/**
		 * The {@link TimeDomain} of the firing timer.
		 */
		TimeDomain timeDomain();
	}
}
