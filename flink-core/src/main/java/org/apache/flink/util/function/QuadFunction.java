package org.apache.flink.util.function;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Function which takes four arguments.
 *
 * @param <S> type of the first argument
 * @param <T> type of the second argument
 * @param <U> type of the third argument
 * @param <V> type of the fourth argument
 * @param <R> type of the return value
 */
@PublicEvolving
@FunctionalInterface
public interface QuadFunction<S, T, U, V, R> {

	/**
	 * Applies this function to the given arguments.
	 *
	 * @param s the first function argument
	 * @param t the second function argument
	 * @param u the third function argument
	 * @param v the fourth function argument
	 * @return the function result
	 */
	R apply(S s, T t, U u, V v);
}
