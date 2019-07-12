package simsearch.examples.sources;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simsearch.examples.util.KeyedDataPoint;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class SampledSourceFunction implements SourceFunction<KeyedDataPoint<Double>> {


    private volatile boolean isRunning = true;

    private String key;
    private String dataFileName;
    private long samplingRate = 0L;

    private volatile long currentTimeMs = -1;
    private Logger LOG = LoggerFactory.getLogger(SampledSourceFunction.class);

    public SampledSourceFunction(String keyValue, String fileName, long samplingRate) {
        this.key = keyValue;
        this.dataFileName = fileName;
        this.samplingRate = samplingRate;

    }

    public void run(SourceFunction.SourceContext<KeyedDataPoint<Double>> sourceContext) {
        // Set an arbitrary starting time, or set one fixed one to always generate the same time stamps (for testing)
        long startTime = 0L;//System.currentTimeMillis(); //calendar.getTimeInMillis();

       /* Get the Stream of sampled data Elements in the File:
        // Here it simulates streaming
        // Here the program simulates stream source, reading the file 10 times*/

        System.out.println("Start reading data");
        double periodMs = Math.round((1.0 / this.samplingRate) * 1000);  // period in miliseconds

        double val = 0.0;

        Scanner scan;
        File file = new File(this.dataFileName);

        try {
            scan = new Scanner(file);

            while (scan.hasNext()) {

                try {
                    val = Double.parseDouble(scan.next());
                } catch (NumberFormatException nfe) {

                    val = -0.023;  // replace mising with the ECG mean
                }
                if (this.currentTimeMs == -1) {
                    this.currentTimeMs = startTime;
                } else {
                    this.currentTimeMs = (long) (this.currentTimeMs + periodMs);
                }


                KeyedDataPoint<Double> point = new KeyedDataPoint<Double>(this.key, this.currentTimeMs, val);
                sourceContext.collect(point);

            }
            scan.close();

        } catch (FileNotFoundException e1) {
            LOG.error("File Not FOUND :" + this.dataFileName);
            // e1.printStackTrace();
        }


    }

    public void cancel() {
        this.isRunning = false;
    }


}