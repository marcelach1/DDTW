### DDTW
Distributed DTW

### Running with Apache Flink on the command line:
We test this example with flink-1.8.0
After starting Flink (see basics in: https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html)
```
$FLINK_PATH/bin/start-cluster.sh 
```
Generate the target:
```
mvn clean package
```
Run the program:
```
$FLINK_PATH/bin/flink run -p 8 -c simsearch.examples.ECGExample target/DTW-1.1-SNAPSHOT.jar --pathInput ./src/main/resources/ecg50T.txt --pathOutput ./src/main/resources/ecg_result.txt --sampleRate 1000 --pathPattern ./src/main/resources/ecg_query.txt
```

Output result in ecg_result.txt, looks like:
```
(startTime, endTime, distance, tsKey, latency)
(12140520,12140982,4.16,1,5)
```


### Data
The data included in this repo is a short excerpt of the original file that can be found in: http://www.cs.ucr.edu/~eamonn/ECG_one_day.zip
