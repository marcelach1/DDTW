package simsearch.examples.util;

import java.util.Comparator;

/**
 * Created by Ariane on 27.11.2017.
 */

public class KeyedDataPointComparator implements Comparator<KeyedDataPoint> {

    /*
     * compare function to sort a sequence of KeyedDataPoint
     */
    public int compare(KeyedDataPoint o1, KeyedDataPoint o2) {
        //o1.compareTo( o2 ) < 0« o1 is smaller than o2.
        if (o1.getTimeStampMs() == o2.getTimeStampMs()) return 0;
        if (o1.getTimeStampMs() > o2.getTimeStampMs()) return 1;
        if (o1.getTimeStampMs() < o2.getTimeStampMs()) return -1;
        return 0;
    }
}

