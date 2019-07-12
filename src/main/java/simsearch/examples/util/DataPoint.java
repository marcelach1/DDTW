package simsearch.examples.util;

//taken from oscon example
/*
POJO to present a data point with multiple attributes (we need time)
check on Flink requirements for POJOS
 */

public class DataPoint<T> {

    private long timeStampMs;
    private T value;

    // CONSTRUCTORS

    public DataPoint() {
        this.timeStampMs = 0;
        this.value = null;
    }

    public DataPoint(long timeStampMs, T value) {
        this.timeStampMs = timeStampMs;
        this.value = value;
    }

    // GETTER AND SETTER

    public long getTimeStampMs() {
        return timeStampMs;
    }

    public void setTimeStampMs(long timeStampMs) {
        this.timeStampMs = timeStampMs;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public <R> DataPoint<R> withNewValue(R newValue) {
        return new DataPoint<R>(this.getTimeStampMs(), newValue);
    }

    public <R> KeyedDataPoint<R> withNewKeyAndValue(String key, R newValue) {
        return new KeyedDataPoint<R>(key, this.getTimeStampMs(), newValue);
    }


    @Override
    public String toString() {
        return "simsearch.examples.util.DataPoint(timestamp=" + timeStampMs + ", value=" + value + ")";
    }
}
