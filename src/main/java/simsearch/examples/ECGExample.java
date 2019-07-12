package simsearch.examples;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import simsearch.examples.SimilaritySearch.DTWwithZnorm;
import simsearch.examples.sources.SampledSourceFunction;
import simsearch.examples.util.KeyedDataPoint;
import simsearch.examples.util.KeyedDataPointTimestampAssignerWM;
import simsearch.examples.util.ECGOutputTransformer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
  --pathInput
  ./src/main/resources/ecg50T.txt
  --pathOutput
  ./src/main/resources/ecg_result.txt
  --sampleRate
  1000
  --pathPattern
  ./src/main/resources/ecg_query.txt
  --Sources
  1
 */

public class ECGExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        List<KeyedDataPoint<Double>> pattern = new ArrayList<KeyedDataPoint<Double>>();
        /*
           Parameters:
        1) required parameters
         */

        String inputPath = params.getRequired("pathInput");
        String patternPath = params.getRequired("pathPattern");
        String outputPath = params.getRequired("pathOutput");
        long sampleRate = Long.parseLong(params.getRequired("sampleRate"));

        // 2) optional parameters with default values

        String distance = params.get("distance", "DTW").trim().toUpperCase(); // distance measurement
        double warpingPath = params.getDouble("warp", 0.1);
        if (warpingPath == 0.0) distance = "ED";
        if (warpingPath >= 1.0) warpingPath = 0.99;
        double distanceMarginBound = params.getDouble("margin", 0.1);   // tolerance interval on initial setting of the query
        double maxDistance = params.getDouble("maxDistance", 100000000.0);
        double maxEuDistance = params.getDouble("maxEuDistance", 100000000.0);
        boolean averageResponseTime = params.getBoolean("art", false);  // set true if output for each window is required (else only matches are outputed)
        String searchType = params.get("searchType", "RQ").toUpperCase();   // RQ (Range Query) or BM (Bestmatch)
        String normType = params.get("normType", "Znorm").trim().toUpperCase(); // normalization type (MinMax or Znorm)
       // read in of pattern (time column is required as otherwise order of pattern is not guarantied on cluster)
        String splitter = params.get("splitter", " "); // field deliminiater
        int colNumber = params.getInt("colNumber", 0); // data column
        int timeCol = params.getInt("timeCol", 1); // time column

        int numSources = params.getInt("Sources", 8); // number of time series to analyze


        // read in of pattern (from file)
        try (BufferedReader br = new BufferedReader(new FileReader(patternPath))) {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                String[] split = sCurrentLine.trim().split(splitter);
                Double sensorid = Double.parseDouble(split[colNumber].trim());
                KeyedDataPoint<Double> dummy = new KeyedDataPoint<Double>("0", Long.parseLong(split[timeCol]), sensorid);
                pattern.add(dummy);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        // derived only parameters:
        int patternsize = pattern.size();
        long periodMs = Math.round((1.0 / sampleRate) * 1000);
        long countTrigger = Math.round(patternsize * (1.1));
        long maxTimeWindow = countTrigger * periodMs;
        long slideOfWindow = params.getLong("SlideSize", maxTimeWindow - Math.round(patternsize * (1 - warpingPath) * periodMs));
        if (slideOfWindow == maxTimeWindow) {
            slideOfWindow = maxTimeWindow - Math.round(patternsize * (0.5));
        }

       /* Check paramters
        System.out.println("PATTERNSIZE " + patternsize);
        System.out.println("periodMs: " + periodMs + " countTrigger: " + countTrigger + "  maxTimeWindow: " + maxTimeWindow + "  slidOfWindow: " + slideOfWindow);
        System.out.println("Time.milliseconds(maxTimeWindow):  " + Time.milliseconds(maxTimeWindow).toMilliseconds());
        System.out.println("Time.milliseconds(slidOfWindow):  " + Time.milliseconds(slideOfWindow).toMilliseconds());
        System.out.println("art " + averageResponseTime);
        */

        // start streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int[] array = new int[numSources];

        for (int i = 1; i <= numSources; i++) {
            array[i - 1] = i;
        }

        // for each sources do:
        for (int i : array) {
            // read the data as a source
            env.addSource(new SampledSourceFunction(Integer.toString(i), inputPath, sampleRate))
                    // assign timestamps and watermarks
                    .assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<KeyedDataPoint<Double>>) new KeyedDataPointTimestampAssignerWM(periodMs))
                    // keyBy (will be mire interesting for multi-dimensional time series)
                    .keyBy("key")
                    // create the window
                    .window(SlidingEventTimeWindows.of(Time.milliseconds(maxTimeWindow), Time.milliseconds(slideOfWindow)))
                    .trigger(CountTrigger.of(countTrigger))
                    // start DTW
                    .process(new DTWwithZnorm(pattern, searchType, warpingPath, maxDistance, maxEuDistance, averageResponseTime, distanceMarginBound))
                    // prepare for output and print to file
                    .map(new ECGOutputTransformer())
                    .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        }

        JobExecutionResult result = env.execute("DTW");
        System.out.println("Time of calculation : " + result.getNetRuntime(TimeUnit.SECONDS) + " s");
    }


}