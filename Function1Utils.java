package com.am.analytics.job.common.utils;

import java.io.Serializable;

import scala.Function1;

/**
 * The Class Function1Utils.
 */
public final class Function1Utils {

    /**
     * Instantiates a new function 1 utils.
     */
    private Function1Utils() {

    }

    /**
     * To function 1.
     *
     * @param <U> the generic type
     * @param <V> the value type
     * @param function the function
     * @return the function 1
     */
    public static <U, V> Function1<U, V> toFunction1(final SerializableFunction<U, V> function) {

        return new SerializableFunc1<U, V>() {

            /**
             * Serialized version of the class.
             */
            private static final long serialVersionUID = 1L;

            @Override
            public <A> Function1<U, A> andThen(final Function1<V, A> arg0) {

                return null;
            }

            @Override
            public V apply(final U arg0) {

                return function.apply(arg0);
            }

            @Override
            public double apply$mcDD$sp(final double arg0) {

                return 0;
            }

            @Override
            public double apply$mcDF$sp(final float arg0) {

                return 0;
            }

            @Override
            public double apply$mcDI$sp(final int arg0) {

                return 0;
            }

            @Override
            public double apply$mcDJ$sp(final long arg0) {

                return 0;
            }

            @Override
            public float apply$mcFD$sp(final double arg0) {

                return 0;
            }

            @Override
            public float apply$mcFF$sp(final float arg0) {

                return 0;
            }

            @Override
            public float apply$mcFI$sp(final int arg0) {

                return 0;
            }

            @Override
            public float apply$mcFJ$sp(final long arg0) {

                return 0;
            }

            @Override
            public int apply$mcID$sp(final double arg0) {

                return 0;
            }

            @Override
            public int apply$mcIF$sp(final float arg0) {

                return 0;
            }

            @Override
            public int apply$mcII$sp(final int arg0) {

                return 0;
            }

            @Override
            public int apply$mcIJ$sp(final long arg0) {

                return 0;
            }

            @Override
            public long apply$mcJD$sp(final double arg0) {

                return 0;
            }

            @Override
            public long apply$mcJF$sp(final float arg0) {

                return 0;
            }

            @Override
            public long apply$mcJI$sp(final int arg0) {

                return 0;
            }

            @Override
            public long apply$mcJJ$sp(final long arg0) {

                return 0;
            }

            @Override
            public void apply$mcVD$sp(final double arg0) {

            }

            @Override
            public void apply$mcVF$sp(final float arg0) {

            }

            @Override
            public void apply$mcVI$sp(final int arg0) {

            }

            @Override
            public void apply$mcVJ$sp(final long arg0) {

            }

            @Override
            public boolean apply$mcZD$sp(final double arg0) {

                return false;
            }

            @Override
            public boolean apply$mcZF$sp(final float arg0) {

                return false;
            }

            @Override
            public boolean apply$mcZI$sp(final int arg0) {

                return false;
            }

            @Override
            public boolean apply$mcZJ$sp(final long arg0) {

                return false;
            }

            @Override
            public <A> Function1<A, V> compose(final Function1<A, U> arg0) {

                return null;
            }
        };
    }

    /**
     * The Class SerializableFunc1.
     *
     * @param <U> the generic type
     * @param <V> the value type
     */
    public static abstract class SerializableFunc1<U, V> implements Serializable, Function1<U, V> {

        /**
         * Serialized Version of the class.
         */
        private static final long serialVersionUID = 1L;

    }
}
