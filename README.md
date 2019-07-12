### DDTW
Implementation of the system described in: "Time Series Similarity Search for StreamingData in Distributed Systems" http://www.redaktion.tu-berlin.de/fileadmin/fg131/Publikation/Papers/Time_Series_Similarity_Search_Darli-2019_crv.pdf

Note:<br>
- The current implementation has been changed to derive the distance threshold from the query, therefore the time it takes for finding the pattern in "One patient, one day, Best Match" mode will take a bit longer than the 37 seconds reported in the paper, the current time is around 62 s.
- The improvement is better reflected when running in "Range query" mode.

The code can be imported in Intellij or generate the jar with maven and run in the command line as explained below:

### Running with Apache Flink on the command line:
We test this example with flink-1.8.0

After starting Flink (see basics in: https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html)
```
$FLINK_PATH/bin/start-cluster.sh 
```
Generate the target (we use: maven 3.6 and Java 8):
```
mvn clean package
```
Run the program:
```
$FLINK_PATH/bin/flink run -p 8 -c simsearch.examples.ECGExample target/DTW-1.1-SNAPSHOT.jar --pathInput ./src/main/resources/ecgSmall.txt --pathOutput ./src/main/resources/ecg_result.txt --sampleRate 1000 --pathPattern ./src/main/resources/ecg_query.txt
```

Output result in ecg_result.txt, looks like:
```
(startTime, endTime, distance, tsKey, latency)
(12140520,12140982,4.16,1,5)
```


### Data
The data included in this repo (ecgSmall.txt) is a short excerpt of the original file that can be found in: http://www.cs.ucr.edu/~eamonn/ECG_one_day.zip
