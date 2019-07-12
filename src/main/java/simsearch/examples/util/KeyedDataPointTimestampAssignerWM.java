package simsearch.examples.util;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by Ariane on 07.03.2018.
 * /**
 * Source: https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/event_timestamps_watermarks.html
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */

public class KeyedDataPointTimestampAssignerWM implements AssignerWithPeriodicWatermarks<KeyedDataPoint<Double>>, org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<KeyedDataPoint<Double>> {

    private long maxOutOfOrderness;

    private long currentMaxTimestamp;


    public KeyedDataPointTimestampAssignerWM(long periodMs) {
        this.maxOutOfOrderness = (periodMs);
    }


    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound

        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Nullable
    public Watermark checkAndGetNextWatermark(KeyedDataPoint<Double> doubleKeyedDataPoint, long l) {
        return null;
    }

    @Override
    public long extractTimestamp(KeyedDataPoint<Double> doubleKeyedDataPoint, long l) {
        long timestamp = doubleKeyedDataPoint.getTimeStampMs();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}

