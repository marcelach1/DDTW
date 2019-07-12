package simsearch.examples.util;

//taken from oscon example
/*
POJO to present a data point with multiple attributes (we need time)
check on Flink requirements for POJOS
 */

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class KeyedDataPoint<T> extends DataPoint<T> implements Serializable {

    private String key;
    private long timeStamp;

    // CONSTRUCTORS

    public KeyedDataPoint() {
        super();
        this.key = null;
        this.timeStamp = 0L;
    }

    public KeyedDataPoint(String key, long timeStampMs, T value) {
        super(timeStampMs, value);
        this.key = key;
        this.timeStamp = System.currentTimeMillis();
    }

    public KeyedDataPoint(String key, long timeStampMs, T value, long timeSystem) {
        super(timeStampMs, value);
        this.key = key;
        this.timeStamp = timeSystem;
    }

    // GETTER AND SETTER

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public <R> KeyedDataPoint<R> withNewValue(R newValue) {
        return new KeyedDataPoint<R>(this.getKey(), this.getTimeStampMs(), newValue);
    }

    @Override
    public String toString() {
        Date date = new Date(getTimeStampMs());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return "ts(" + getKey() + ": " + getTimeStampMs() + "): " + sdf.format(date) + "," + getValue();
    }

}
