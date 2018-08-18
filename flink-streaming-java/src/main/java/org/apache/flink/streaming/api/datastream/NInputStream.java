package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class NInputStream<IN> {

//	private final StreamExecutionEnvironment environment;
//
//	private final DataStream<IN> mainInput;
//
//	private final List<DataStream<?>> sideInputs;
//
//	public NInputStream(
//			final StreamExecutionEnvironment env,
//			final DataStream<IN> mainInput
//	) {
//		this.environment = Preconditions.checkNotNull(env);
//		this.mainInput = Preconditions.checkNotNull(mainInput);
//		this.sideInputs = new ArrayList<>();
//	}
//
//	// TODO: 8/15/18 this can be in the dataStream and maybe in the KeyedStream and
//	// as soon as it is called, it gives an NInputStream.
//	public <INX> void withSideInput(DataStream<INX> input) {
//		sideInputs.add(input);
//	}
//
//	@Internal
//	private <OUT> SingleOutputStreamOperator<OUT> transform(
//			final String functionName,
//			final TypeInformation<OUT> outTypeInfo,
//			final NInputStreamOperator<IN1, IN2, OUT> operator) {
//
//		// read the output type of the input Transforms
//		// to coax out errors about MissingTypeInfo
//		for (DataStream<?> sideInput : sideInputs) {
//			// TODO: 8/15/18
//			sideInput.getType();
//		}
//		// TODO: 8/15/18 go to the StreamGraphGenerator to see what is needed
//		// does it have to support an NInputStream as side input???
//		TwoInputTransformation<IN1, IN2, OUT> transform = new TwoInputTransformation<>(
//				inputStream1.getTransformation(),
//				inputStream2.getTransformation(),
//				functionName,
//				operator,
//				outTypeInfo,
//				environment.getParallelism());
//
//		if (inputStream1 instanceof KeyedStream) {
//			KeyedStream<IN1, ?> keyedInput1 = (KeyedStream<IN1, ?>) inputStream1;
//			TypeInformation<?> keyType1 = keyedInput1.getKeyType();
//			transform.setStateKeySelectors(keyedInput1.getKeySelector(), null);
//			transform.setStateKeyType(keyType1);
//		}
//
//		@SuppressWarnings({ "unchecked", "rawtypes" })
//		SingleOutputStreamOperator<OUT> returnStream = new SingleOutputStreamOperator(environment, transform);
//
//		getExecutionEnvironment().addOperator(transform);
//
//		return returnStream;
//	}
}
