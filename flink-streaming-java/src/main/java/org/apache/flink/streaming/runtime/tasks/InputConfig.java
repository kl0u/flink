package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.operators.MultiInputStreamOperator;
import org.apache.flink.streaming.runtime.io.GeneralValveOutputHandler;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.util.InputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;

public class InputConfig {

	private final TypeInformation<?> inputTypeInfo;

	@Nullable
	private final KeySelector<?, ?> keySelector;

	private final Collection<InputGate> inputGates;

	private final WatermarkGauge watermarkGauge;

	InputConfig(
			final TypeInformation<?> inputTypeInfo,
			@Nullable final KeySelector<?, ?> keySelector
	) {
		this.inputTypeInfo = Preconditions.checkNotNull(inputTypeInfo);
		this.keySelector = keySelector;

		this.inputGates = new ArrayList<>();
		this.watermarkGauge = new WatermarkGauge();
	}

	void addInputGate(final InputGate gate) {
		inputGates.add(gate);
	}

	public Collection<InputGate> getInputGates() {
		return inputGates;
	}

	public WatermarkGauge getWatermarkGauge() {
		return watermarkGauge;
	}

	public TypeInformation<?> getInputTypeInfo() {
		return inputTypeInfo;
	}

	public GeneralValveOutputHandler.OperatorProxy createOperatorProxy(InputTag inputTag, MultiInputStreamOperator<?> operator) {
		return new SingleInputOperatorProxy<>(operator, inputTag, keySelector);
	}
}
