package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class NInputTransformation<IN, OUT> extends StreamTransformation<OUT> {

	// TODO: 8/16/18 so far we ignore the main stream???

	private final List<StreamTransformation<?>> inputTransformations;

	/**
	 * Creates a new {@code StreamTransformation} with the given name, output type and parallelism.
	 *
	 * @param name        The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
	 * @param outputType  The output type of this {@code StreamTransformation}
	 * @param parallelism The parallelism of this {@code StreamTransformation}
	 */
	public NInputTransformation(
			final String name,
			final List<StreamTransformation<?>> inputs,
			final TypeInformation<OUT> outputType,
			final int parallelism) {

		super(name, outputType, parallelism);
		this.inputTransformations = Preconditions.checkNotNull(inputs);
	}

	public List<StreamTransformation<?>> getInputTransformations() {
		return Collections.unmodifiableList(inputTransformations);
	}


	/**
	 * Returns the {@code TypeInformation} for each of the side-inputs.
	 */
	public List<TypeInformation<?>> getInputTypes() {
		final List<TypeInformation<?>> typeInformation = new ArrayList<>();
		for (StreamTransformation<?> transformation : inputTransformations) {
			typeInformation.add(transformation.getOutputType());
		}
		return typeInformation;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {

	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		return null;
	}
}
