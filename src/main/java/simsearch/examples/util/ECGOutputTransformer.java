package simsearch.examples.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;


/*
    Ariane:  Calculates latency of output
 */

public class ECGOutputTransformer implements MapFunction<Tuple5<Long, Long, Double, String, Long>, Tuple5<Long, Long, Double, String, Long>> {


    @Override
    public Tuple5<Long, Long, Double, String, Long> map(Tuple5<Long, Long, Double, String, Long> input) {

        input.f4 = System.currentTimeMillis() - input.f4;

        return new Tuple5<>(input.f0, input.f1, input.f2, input.f3, input.f4);
    }

}
