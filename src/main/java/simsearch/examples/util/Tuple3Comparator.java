package simsearch.examples.util;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Comparator;

/**
 * Created by Ariane on 27.11.2017.
 * Sort Tuple3 among the Double value which is the distance, smallest distance on top
 */

public class Tuple3Comparator implements Comparator<Tuple3<Long, Long, Double>> {

    /*
     * compare function to sort the resulting tuples of Similarity Search by their distance
     */
    @Override
    public int compare(Tuple3<Long, Long, Double> o1, Tuple3<Long, Long, Double> o2) {

        if (o1.f2 > o2.f2) return 1;
        if (o1.f2 < o2.f2) return -1;
        if (o1.f2 == o2.f2) return 0;

        return 0;
    }
}

