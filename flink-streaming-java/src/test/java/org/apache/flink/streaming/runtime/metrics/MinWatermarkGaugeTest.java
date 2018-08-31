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

package org.apache.flink.streaming.runtime.metrics;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link MinWatermarkGauge}.
 */
public class MinWatermarkGaugeTest {

	@Test
	public void testSetCurrentLowWatermark() {
		WatermarkGauge metric1 = new WatermarkGauge();
		WatermarkGauge metric2 = new WatermarkGauge();
		WatermarkGauge metric3 = new WatermarkGauge();
		WatermarkGauge metric4 = new WatermarkGauge();

		final MinWatermarkGauge metric =
				new MinWatermarkGauge(metric1, metric2, metric3, metric4);

		Assert.assertEquals(Long.MIN_VALUE, metric.getValue().longValue());

		metric1.setCurrentWatermark(1L);
		Assert.assertEquals(Long.MIN_VALUE, metric.getValue().longValue());

		metric2.setCurrentWatermark(2L);
		Assert.assertEquals(Long.MIN_VALUE, metric.getValue().longValue());

		metric3.setCurrentWatermark(0L);
		Assert.assertEquals(Long.MIN_VALUE, metric.getValue().longValue());

		metric4.setCurrentWatermark(0L);
		Assert.assertEquals(0L, metric.getValue().longValue());

		metric3.setCurrentWatermark(3L);
		Assert.assertEquals(0L, metric.getValue().longValue());

		metric4.setCurrentWatermark(3L);
		Assert.assertEquals(1L, metric.getValue().longValue());

		metric1.setCurrentWatermark(4L);
		Assert.assertEquals(2L, metric.getValue().longValue());

		metric2.setCurrentWatermark(5L);
		Assert.assertEquals(3L, metric.getValue().longValue());
	}
}
