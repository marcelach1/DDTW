### DDTW
Implementation of the system described in: "Time Series Similarity Search for StreamingData in Distributed Systems" http://www.redaktion.tu-berlin.de/fileadmin/fg131/Publikation/Papers/Time_Series_Similarity_Search_Darli-2019_crv.pdf

Note:<br>
- The current implementation has been changed to derive the distance threshold from the query, therefore the time it takes for finding the pattern in "One patient, one day, Best Match" mode will take a bit longer than the 37 seconds reported in the paper, the current time is around 62 s.
- The changes included in this code are intended to improve the performance in "Range query" mode, big sets and in a cluster.

The code can be run locally (tested in a laptop), import the code in Intellij or generate the jar with maven and execute it in the command line as explained below:

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
$FLINK_PATH/bin/flink run -p 8 -c simsearch.examples.ECGExample target/DTW-1.1-SNAPSHOT.jar \
                     --pathInput $USER_PATH/src/main/resources/ecgSmall.txt \
                     --pathOutput $USER_PATH/src/main/resources/ecg_result \
                     --sampleRate 1000 \
                     --pathPattern $USER_PATH/src/main/resources/ecg_query.txt
```

Output result in ecg_result, looks like:
```
#(startTime, endTime, distance, tsKey, latency)
...
(2474640,2475102,7.91,ecg_patient1,6)
(2661120,2661582,3.26,ecg_patient1,8)  <-- best match
(2781324,2781786,4.6,ecg_patient1,6)
(2866248,2866710,7.29,ecg_patient1,5)
(7140504,7140966,4.14,ecg_patient1,6)
(14398020,14398482,6.33,ecg_patient1,7)
```


### Data
The data included in this repo (ecgSmall.txt) is a short excerpt of the original file that can be found in: http://www.cs.ucr.edu/~eamonn/ECG_one_day.zip
