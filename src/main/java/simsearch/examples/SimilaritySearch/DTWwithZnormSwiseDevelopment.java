package simsearch.examples.SimilaritySearch;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import simsearch.examples.util.KeyedDataPoint;
import simsearch.examples.util.KeyedDataPointComparator;
import simsearch.examples.util.Tuple3Comparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.abs;

/**
 * Created by Ariane on 06.11.2017.
 * t_0, t_1,....t_80  and a pattern p_0, p_1,....p_4
 * patternsize = 5
 * margin = 20%
 * DTW for t_0 - t_6(0+5*1,2=6)
 * t_1 - t_7(1+5*1,2=7)
 * t_76 - t_80 (80-76= 4 == 5*(1-0,2))
 * <p>
 * as we think here in distances and similarity measures, we define a distance is postive and -1.0 is therefore a pruning
 * default value, indicating that this path is not simliar enough.
 */
public class DTWwithZnormSwiseDevelopment extends ProcessWindowFunction<KeyedDataPoint<Double>, Tuple5<Long, Long, Double, String, Long>, Tuple, TimeWindow> {
    // need to be non-static!!!!!!!
    // contains the pattern as List of KeyedDataPoints
    private transient ValueState<Tuple20<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> distanceThresholds;
    private double[] pattern; // contains the pattern as List of KeyedDataPoints
    //default value
    private double relWarpingWindow = 0.1;
    private String searchTyp = "";
    //checks if the distance is set
    private boolean setMaxDistance = false;
    // bool to remember if the distance was given or not
    private boolean maxDistanceSetByUser = false;
    private double definedMaxDistance = 100000000.0;
    private double definedMaxEuDistance = 100000000.0;
    private boolean art = false; // print latency for pruned windows
    private double distanceMargin = 0.1;
    private double distanceMarginBound = 0.1;
    // calculated later in the programm
    private int patternsize;
    private int warpingWindow;
    private int maxLengthPattern;
    private int minLengthPattern;
    private Tuple2<Integer, Double> maxValue;

    /*
     * constructors
     * attention if maxDistance is set, also change this.setMaxDistance to true and allow to set distance margin
     * order is important
     * sortPattern always as last step
     */

    public DTWwithZnormSwiseDevelopment(List<KeyedDataPoint<Double>> pattern, String searchTyp, double relWarpingWindow, double maxDistance, double maxEuDistance, boolean art, double distanceMarginBound) {
        this.relWarpingWindow = relWarpingWindow;
        this.definedMaxDistance = maxDistance;
        this.definedMaxEuDistance = maxEuDistance;
        this.art = art;
        if (maxDistance < 100000000.0) this.maxDistanceSetByUser = true;
        this.searchTyp = searchTyp;
        this.distanceMarginBound = distanceMarginBound;
        sortPattern(pattern);
        if (searchTyp.equals("BM")) this.distanceMargin = 0.0;
    }


    /**
     * apply calculates the  DTW distance
     *
     * @param tuple     =
     * @param iterable  = subsequence
     * @param collector return/output the start and end of the a similar sequence and the related distance
     */
    @Override
    public void process(Tuple tuple, Context context, Iterable<KeyedDataPoint<Double>> iterable, Collector<Tuple5<Long, Long, Double, String, Long>> collector) throws Exception {
        Tuple20<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> distanceHistory = distanceThresholds.value();
        double maxEuDistance = Math.min(distanceHistory.f19, this.definedMaxEuDistance);
        double maxDTWDistance = Math.min(this.definedMaxDistance, Math.min(distanceHistory.f19, distanceHistory.f9));

        // new Array for sequence to check and final output
        List<KeyedDataPoint<Double>> subsequence = new ArrayList<KeyedDataPoint<Double>>();
        List<KeyedDataPoint<Double>> subsequence2 = new ArrayList<KeyedDataPoint<Double>>();
        List<Tuple3<Long, Long, Double>> resultList = new ArrayList<Tuple3<Long, Long, Double>>();

        /** z-norm sequence */
        double sum = 0;

        // get the relevant aggregations
        for (KeyedDataPoint<Double> x : iterable) {
            subsequence2.add(x);
            sum += x.getValue();
        }

        int subsequenceSize = subsequence2.size();
        double mean = sum / (double) subsequenceSize;
        double var = 0.0;

        // get variance value
        for (KeyedDataPoint<Double> x : subsequence2) {
            var += ((x.getValue() - mean) * (x.getValue() - mean));
        }

        var = Math.sqrt(var / ((double) subsequenceSize - 1));

        //sort the sequence by time
        subsequence2.sort(new KeyedDataPointComparator());

        // z-normalize each point
        for (int i = 0; i < subsequenceSize; i++) {
            KeyedDataPoint<Double> x = subsequence2.get(i);
            double Xnorm = (x.getValue() - mean) / var;
            subsequence.add(i, new KeyedDataPoint<>(x.getKey(), x.getTimeStampMs(), Xnorm));
        }

        /** DTW */
        // 1) geta valid subsequences
        for (int point = 0; point < subsequenceSize; point++) {

            // we add to the point the length of the pattern incl the extension margin of the query length
            int maxLength = point + this.maxLengthPattern;

            // as long as this length is in the current subsequence, go ahead, else set maxLength to length
            if (maxLength > subsequenceSize) {
                maxLength = subsequenceSize;
            }

            /** 2) PRUNING with ED distance */
            // sequences has the right length
            if (subsequence.subList(point, maxLength).size() >= this.minLengthPattern && subsequence.subList(point, maxLength).size() <= this.maxLengthPattern) {
                // subList= fromIndex, inclusive, and toIndex, exclusive

                double euDist = 0;
                for (int i = 0; i < this.patternsize; i++) {

                    if (i + point < maxLength)
                        euDist = euclideanDistanceSimple(subsequence.get(i + point).getValue(), this.pattern[i], euDist);
                }

                // if sequence is valid, start DTW distance matrix
                if (euDist < maxEuDistance) {
                    /** DTW distance matrix */
                    double distance[][] = calcMatrix(subsequence.subList(point, maxLength), maxDTWDistance * (1 + this.distanceMargin));

                    if (distance.length > 1 && distance[0].length > 1) {
                        //printResult(distance);
                        Tuple2<Double, Integer> minimaldistance = findMinDistanceofMatrix(distance);

                        // minimal distance of matrix is below the threshold
                        if (minimaldistance.f0 <= maxDTWDistance * (1 + this.distanceMargin) && minimaldistance.f0 >= 0.0) {
                            // shall distance value be updated ?
                            if (Math.ceil(minimaldistance.f0) != Math.ceil(maxDTWDistance))
                                developDistance(minimaldistance.f0, maxDTWDistance, euDist, maxEuDistance, distanceHistory);

                            resultList.add(new Tuple3<Long, Long, Double>(subsequence.get(0).getTimeStampMs(), subsequence.get(subsequence.size() - 1).getTimeStampMs(), Precision.round(minimaldistance.f0, 2)));

                            /** optional: if one window has a similar sequence, stop search (time performance) and announce it as a match*/
                            // point = subsequenceSize;

                        }
                    }
                }// DTW IF
            } else point = subsequenceSize; // window is done if this condition does not hold anymore
        }

        if (!resultList.isEmpty()) {
            // here we only take the timestamp of the last obs., later we use this to calculate the latency in ECGoutputtransformer
            resultList.sort(new Tuple3Comparator());
            long latency = subsequence.get(subsequence.size() - 1).getTimeStamp();
            collector.collect(new Tuple5<>(resultList.get(0).f0, resultList.get(0).f1, resultList.get(0).f2, subsequence.get(0).getKey(), latency));

        } else {
            // this output is only required for latency experiements, set art to true
            if (this.art) {
                long latency = subsequence.get(subsequence.size() - 1).getTimeStamp();
                Long startwindow = subsequence.get(0).getTimeStampMs();
                Long endwindow = subsequence.get(subsequence.size() - 1).getTimeStampMs();
                collector.collect(new Tuple5<>(startwindow, endwindow, 0.0, subsequence.get(0).getKey(), latency));
            }
        }
    }

    ///----------------------DISTANCE CALCULATIONS -----------------------------------------------

    /**
     * calcs Euclidean Distance and sums up an additional value sumDistance
     * with distance = sqrt(sum_n((x_n-y_n)^2)), where we ignore sqrt()
     *
     * @param i, j two double values + sumDistance double to add on top
     * @return distance as double
     */
    private double euclideanDistanceSimple(Double i, Double j, double sumDistance) {
        double distance = Math.pow(i - j, 2);
        return distance + sumDistance;
    }

    /**
     * calcs Euclidean Distance
     * with distance = sqrt(sum_n((x_n-y_n)^2)), where we ignore sqrt()
     * additionally it searches the smallest neighbor and adds its distance to the current two points
     *
     * @param distances the current distances matrix
     * @param irow      and jrow the position if i and j in the matrix
     * @param i,        j two double values
     * @return distance as double
     */
    private double euclideanDistance3(double i, double j, double[][] distances, int irow, int jrow, double maxDistance) {

        double distanceFin = -1.0;
        double minNeighbour = findMinimalNeighbour(distances, irow, jrow, maxDistance);

        if (minNeighbour >= 0.0) {
            double distance = Math.pow(i - j, 2);
            if (distance < maxDistance) {
                distanceFin = distance + minNeighbour;
                if (distanceFin > maxDistance) distanceFin = -1.0;
            }
        }

        return distanceFin;
    }

    /**
     * searches for the minimal distanc in the top line of the matrix (pattern = patternsize)
     * distance[patternsize][subsequencesize]
     */
    private Tuple2<Double, Integer> findMinDistanceofMatrix(double[][] distance) {
        int endpoint = distance[0].length - 1;

        // set mindistance of sequence to (n,m)
        double minimaldistance = distance[distance.length - 1][distance[1].length - 1];
        // check all distances within the warping path
        for (int j = distance[0].length - 1; j >= this.minLengthPattern; j--) {

            double dummy = distance[distance.length - 1][j];
            if ((dummy < minimaldistance && dummy >= 0.0) || (minimaldistance < 0.0 && dummy >= 0.0)) {
                minimaldistance = dummy;
                endpoint = j;

            }
            if (minimaldistance == 0.0) break;
        }
        return new Tuple2<Double, Integer>(minimaldistance, endpoint);
    }

    // prints out the distance matrix
    private void printResult(double[][] distance) {

        for (int k = distance.length - 1; k >= 0; k--) {
            System.out.println(",");
            for (int j = distance[0].length - 1; j >= 0; j--) {
                System.out.print(distance[k][j] + "( " + k + "," + j + ")" + ",");
            }
        }
        System.out.println(" ");
    }

    /**
     * calculates the  DTW matrix step by step to prune immediately and prevent unnecessary calculations
     */
    private double[][] calcMatrix(List<KeyedDataPoint<Double>> target, double maxDistance) {

        double[][] dist = new double[patternsize][target.size()];
        boolean breaked = false;
        double distance00 = 0.0;

        int maxIndexQ = this.maxValue.f0;
        double pruneDist = 0.0;
        if (target.size() > maxIndexQ) {
            pruneDist = euclideanDistanceSimple(target.get(maxIndexQ).getValue(), maxValue.f1, 0.0);
        }


        if (pruneDist < maxDistance * 0.5) { // heuristic for max point of Q, should not cause a distance larger than 50% of the threshold

            for (int i = 0; i < patternsize; i++) {
                // path is a bool that is set to true in case we find at least one value
                // for this specific pattern[i] which is below the maximal distance
                boolean path = false;
                for (int j = 0; j < target.size(); j++) {
                    // start of matrix
                    if (i == 0 && j == 0) {
                        distance00 = euclideanDistanceSimple(pattern[0], target.get(0).getValue(), 0.0);

                        if (distance00 >= maxDistance) {
                            // if the first distance is already larger than the minDistance -> prune
                            // LB_KimFL
                            i = patternsize;
                            breaked = true;
                            break;
                        } else {
                            dist[i][j] = distance00;
                            path = true;
                        }
                    }

                    if (((i == 0 && j == 1) || (i == 1 && j == 0)) && !breaked) {
                        double currentDist = euclideanDistanceSimple(pattern[i], target.get(j).getValue(), distance00);
                        // I can not prune here, because that is only one direction
                        if (currentDist >= maxDistance) {
                            if (i == 0) {
                                for (int k = j; k < target.size(); k++) {
                                    dist[i][k] = -1.0;
                                }
                            }
                            break;
                        } else {
                            dist[i][j] = currentDist;
                            dist[0][0] = distance00;
                            path = true;
                        }
                    }
                    // the rest of the matrix after the special calculations for the beginning
                    if (i > 1 || j > 1) {
                        if (!breaked && abs(i - j) <= warpingWindow) {
                            double currentDist =
                                    euclideanDistance3(pattern[i], target.get(j).getValue(), dist, i, j, maxDistance);

                            if (currentDist > maxDistance || currentDist < 0.0) {
                                dist[i][j] = -1.0;
                            } else {
                                dist[i][j] = currentDist;
                                path = true;
                            }
                        }
                        if (!breaked && abs(i - j) > warpingWindow) {
                            dist[i][j] = -1.0;
                        }
                    }

                    if (!path && j == target.size() - 1) {
                        i = patternsize;
                        breaked = true;
                        break;
                    }
                } // end for subsequence
            }
        } // end for pattern
        else {
            breaked = true;
        }
        if (breaked) return new double[1][1];

        return dist;
    }

    /**
     * find the minimal ancestor for the position irwo, jrow
     */
    private double findMinimalNeighbour(double[][] distances, int irow, int jrow, double maxDistance) {

        double minNeighbor = -1.0;

        if (jrow - 1 >= 0 && irow - 1 >= 0) {
            minNeighbor = minPos(distances[irow - 1][jrow], distances[irow][jrow - 1], distances[irow - 1][jrow - 1]);
        }
        if (jrow - 1 >= 0 && irow - 1 < 0) {
            if (distances[irow][jrow - 1] >= 0.0 && distances[irow][jrow - 1] < maxDistance)
                minNeighbor = distances[irow][jrow - 1];

            else minNeighbor = -1.0;
        }
        if (irow - 1 >= 0 && jrow - 1 < 0) {
            if (distances[irow - 1][jrow] >= 0.0 && distances[irow - 1][jrow] < maxDistance)
                minNeighbor = distances[irow - 1][jrow];

            else minNeighbor = -1.0;
        }

        return minNeighbor;
    }

    private double minPos(double x, double y, double z) {
        // as we thing in distances, which are postive values, we prune by using -1.0
        double min = -1.0;
        if (x >= 0.0) min = x;
        if ((y >= 0.0 && y < min) || (y >= 0.0 && min == -1.0)) min = y;
        if ((z >= 0.0 && z < min) || (z >= 0.0 && min == -1.0)) min = z;

        return min;
    }


    //--------SORTING AND PREPARING THE Pattern AND INITIAL THRESHOLD
    /**
     * sorting the pattern/ KeyedDataPoint
     */
    private void sortPattern(List<KeyedDataPoint<Double>> pattern) {
        this.patternsize = pattern.size();
        this.pattern = new double[pattern.size()];
        pattern.sort(new KeyedDataPointComparator());
        // maximal subsequence to check
        this.maxLengthPattern = (int) Math.round(this.patternsize * (1 + this.relWarpingWindow));
        this.minLengthPattern = (int) Math.round(this.patternsize * (1 - this.relWarpingWindow));
        if (this.minLengthPattern < this.patternsize * 0.5) // should not be smaller (see related work )
            this.minLengthPattern = (int) Math.round(this.patternsize * 0.5);

        this.warpingWindow = (int) Math.round(this.patternsize * (this.relWarpingWindow));
        double patternExtention = 0;

        // z-norm pattern
        double sum = 0;
        double max = Math.abs(pattern.get(0).getValue());
        int maxIndex = 0;

        // get the relevant aggregations
        for (KeyedDataPoint<Double> x : pattern) {
            sum += x.getValue();
            if (abs(x.getValue()) > max) {
                max = abs(x.getValue());
                maxIndex = pattern.indexOf(x);
            }

        }

        double mean = sum / (double) this.patternsize;
        double var = 0.0;
        // get the variance
        for (KeyedDataPoint<Double> x : pattern) {
            var += ((x.getValue() - mean) * (x.getValue() - mean));
        }
        var = Math.sqrt(var / ((double) this.patternsize - 1));


        //normalize query
        for (int i = 0; i < this.patternsize; i++) {
            KeyedDataPoint<Double> x = pattern.get(i);
            double Xnorm;
            if (var == 0.0) Xnorm = 0.0;
            else Xnorm = (x.getValue() - mean) / var;
            this.pattern[i] = Xnorm;
            patternExtention = euclideanDistanceSimple(Xnorm, Xnorm * (1 + (this.distanceMarginBound * 2)), patternExtention);

        }

        // set the initial threshold values to prune
        if (!this.maxDistanceSetByUser) {
            this.definedMaxDistance = patternExtention;
            this.definedMaxEuDistance = patternExtention * 10; // another heuristic, we need to extend ED as we also extend the aubsequence length
            //  this.definedMaxEuDistance = patternExtention + (this.patternsize*this.relWarpingWindow*2)*euclideanDistanceSimple(this.pattern[maxIndex],this.pattern[maxIndex] * (1 + (this.distanceMarginBound * 2)),0); // to sharp
            this.setMaxDistance = true;
        }

        this.maxValue = new Tuple2<>(maxIndex, this.pattern[maxIndex]);
    }
    //-----------------------------------------------------DISTANCE DEVELOPMENT

    /**
     * Develop the maximal distance over the calculation
     */
    private void developDistance(double newDTWDistance, double maxDTWDistance, double newEDDistance, double maxEDDistance, Tuple20<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> distanceDummy) throws IOException {
        boolean updateED = false;
        boolean updateDTW = false;

        newDTWDistance = Math.ceil(newDTWDistance);
        maxDTWDistance = Math.ceil(maxDTWDistance);
        //System.out.println("DTW: New:  " + newDTWDistance + ", max: " + maxDTWDistance);
        //System.out.println("ED: New:  " + newEDDistance + ", max: " + maxEDDistance);

        if (this.maxDistanceSetByUser) {
            // if the user give a distances
            if (searchTyp.equals("BM")) {
                // for BESTMATCH we still take the best found distance
                if (newDTWDistance < maxDTWDistance) {
                    maxDTWDistance = newDTWDistance;
                    updateDTW = true;
                }
            }
            if (searchTyp.equals("RQ")) {
                //do nothing
            }

        } else { // no user given distance
            double[] distances = tupleSum(distanceDummy);
            double sumDTWold = distances[0];
            double sumDTWnew = distances[1] + newDTWDistance;


            if (sumDTWold <= sumDTWnew) {
                // do nothing as trend is increasing or is static
                // the newDistance found is larger than the maxDistance
                if (sumDTWnew / sumDTWold < 1.1) {
                    distanceDummy.f9 = maxDTWDistance;
                    updateDTW = true;
                }


                if (distanceDummy.f8 == 0.0 || distanceDummy.f7 == 0.0 || distanceDummy.f6 == 0.0 || distanceDummy.f5 == 0.0 || distanceDummy.f4 == 0.0 || distanceDummy.f3 == 0.0 || distanceDummy.f2 == 0.0 || distanceDummy.f1 == 0.0 || distanceDummy.f0 == 0.0) {
                    maxDTWDistance = newDTWDistance;
                    updateDTW = true;

                }
            }
            //old is larger than the new mean
            else {

                if (searchTyp.equals("BM")) {
                    if (newDTWDistance < maxDTWDistance) {
                        maxDTWDistance = newDTWDistance;
                        updateDTW = true;
                    }
                }
                //if Range Queries are searched
                if (searchTyp.equals("RQ")) {
                    // variance of the old values compared to the new distance
                    double varDTW = (Math.pow(distanceDummy.f0 - newDTWDistance, 2) + Math.pow(distanceDummy.f1 - newDTWDistance, 2) + Math.pow(distanceDummy.f2 - newDTWDistance, 2) + Math.pow(distanceDummy.f3 - newDTWDistance, 2) + Math.pow(distanceDummy.f4 - newDTWDistance, 2) + Math.pow(distanceDummy.f5 - newDTWDistance, 2) + Math.pow(distanceDummy.f6 - newDTWDistance, 2) + Math.pow(distanceDummy.f7 - newDTWDistance, 2) + Math.pow(distanceDummy.f8 - newDTWDistance, 2) + Math.pow(distanceDummy.f9 - newDTWDistance, 2)) / 10;
                    varDTW = Math.sqrt(varDTW);

                    if (sumDTWnew / sumDTWold < 0.1) {
                        distanceDummy = new Tuple20(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, distanceDummy.f10, distanceDummy.f11, distanceDummy.f12, distanceDummy.f13, distanceDummy.f14, distanceDummy.f15, distanceDummy.f16, distanceDummy.f17, distanceDummy.f18, distanceDummy.f19);
                        maxDTWDistance = newDTWDistance;
                        maxEDDistance = newEDDistance;
                        updateDTW = true;
                        updateED = true;
                    } else {
                        if ((sumDTWnew <= sumDTWold - (sumDTWold / 10)) || varDTW > newDTWDistance * 0.5) {
                            // postive we lost 1/10 of the sum or the distance to the new distance is the its half
                            if (newDTWDistance < maxDTWDistance) {
                                maxDTWDistance = ((maxDTWDistance * 0.5) + (newDTWDistance * 0.5));
                                updateDTW = true;
                            }
                            if (newDTWDistance > maxDTWDistance) {

                                if (newDTWDistance <= maxDTWDistance * this.distanceMargin && newDTWDistance >= maxDTWDistance) {
                                    maxDTWDistance = ((maxDTWDistance * 0.75) + (newDTWDistance * 0.25));
                                    updateDTW = true;
                                }
                            }
                        } else {
                            // we do nothing as no significant change appeared
                        }
                    }

                    maxDTWDistance = Math.ceil(maxDTWDistance);
                    newEDDistance = Math.ceil(newEDDistance);
                    maxEDDistance = Math.ceil(maxEDDistance);
                }
            }
            if (updateDTW) {

                double sumEDnew = distances[3] + newEDDistance;
                double sumEDold = distances[2];

                if (distanceDummy.f18 == 0.0 || distanceDummy.f17 == 0.0 || distanceDummy.f16 == 0.0 || distanceDummy.f15 == 0.0 || distanceDummy.f14 == 0.0 || distanceDummy.f13 == 0.0 || distanceDummy.f12 == 0.0 || distanceDummy.f11 == 0.0 || distanceDummy.f10 == 0.0) {
                    maxEDDistance = Math.ceil(newEDDistance);
                    updateED = true;
                } else {

                    // the newDistance found is larger than the maxDistance
                    maxEDDistance = updateEDDistance(newEDDistance, maxEDDistance);
                    updateED = true;

                }

               if (maxEDDistance < (sumEDnew / (distances[7] + 1))) maxEDDistance = (sumEDnew / (distances[7] + 1));
            }


        }

        if (updateDTW & !updateED) {
            maxDTWDistance = Math.ceil(maxDTWDistance);
            distanceThresholds.update(new Tuple20(distanceDummy.f1, distanceDummy.f2, distanceDummy.f3, distanceDummy.f4, distanceDummy.f5, distanceDummy.f6, distanceDummy.f7, distanceDummy.f8, distanceDummy.f9, maxDTWDistance, distanceDummy.f10, distanceDummy.f11, distanceDummy.f12, distanceDummy.f13, distanceDummy.f14, distanceDummy.f15, distanceDummy.f16, distanceDummy.f17, distanceDummy.f18, distanceDummy.f19));
            this.definedMaxDistance = Math.ceil(maxDTWDistance);
            }
        if (updateDTW & updateED) {
            maxDTWDistance = Math.ceil(maxDTWDistance);
            maxEDDistance = Math.ceil(maxEDDistance);
            distanceThresholds.update(new Tuple20(distanceDummy.f1, distanceDummy.f2, distanceDummy.f3, distanceDummy.f4, distanceDummy.f5, distanceDummy.f6, distanceDummy.f7, distanceDummy.f8, distanceDummy.f9, maxDTWDistance, distanceDummy.f11, distanceDummy.f12, distanceDummy.f13, distanceDummy.f14, distanceDummy.f15, distanceDummy.f16, distanceDummy.f17, distanceDummy.f18, distanceDummy.f19, maxEDDistance));
            this.definedMaxDistance = Math.ceil(maxDTWDistance);
            this.definedMaxEuDistance = Math.ceil(maxEDDistance);
            }

    }


    // create history summary
    private double[] tupleSum(Tuple20<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> distanceDummy) {
        double[] distance = new double[8];
        boolean zeros1 = false;
        boolean zeros2 = false;

        for (int i = 0; i < 20; i++) {
            if ((double) distanceDummy.getField(i) > 0.0) {
                if (i < 10) {
                    distance[0] = distance[0] + (double) distanceDummy.getField(i);
                    distance[4] = distance[4] + 1;
                }
                if (i < 10 && i > 0) {
                    distance[1] = distance[1] + (double) distanceDummy.getField(i);
                    distance[6] = distance[6] + 1;
                }
                if (i >= 10) {

                    distance[2] = distance[2] + (double) distanceDummy.getField(i);
                    distance[5] = distance[5] + 1;
                }
                if (i > 10) {
                    distance[3] = distance[3] + (double) distanceDummy.getField(i);
                    distance[7] = distance[7] + 1;

                }
            } else {

                if (!zeros1 && i < 10) {
                    distance[6] = distance[6] + 1;
                    zeros1 = true;
                }
                if (!zeros2 && i >= 10) {
                    distance[7] = distance[7] + 1;
                    zeros2 = true;

                }

            }
        }

        return distance;

    }

    // separate update for ED
    private double updateEDDistance(double newEDDistance, double maxEDDistance) {

        if (newEDDistance > maxEDDistance) {

            if (newEDDistance <= maxEDDistance * this.distanceMargin && newEDDistance >= maxEDDistance)
                maxEDDistance = ((maxEDDistance * 0.5) + (newEDDistance * 0.5));

            if (newEDDistance <= maxEDDistance * 2 * this.distanceMargin && newEDDistance >= maxEDDistance)
                maxEDDistance = ((maxEDDistance * 0.75) + (newEDDistance * 0.25));
        }
        // the new distance is smaller than the maxDistance
        if (newEDDistance < maxEDDistance) {
            if (newEDDistance * 2 <= maxEDDistance) {
                maxEDDistance = ((maxEDDistance * 0.25) + (newEDDistance * 0.75));
            } else {
                if (newEDDistance >= maxEDDistance * 0.8 && newEDDistance <= maxEDDistance * 0.9)
                    maxEDDistance = ((maxEDDistance * 0.5) + (newEDDistance * 0.5));
                else {
                    maxEDDistance = ((maxEDDistance * 0.75) + (newEDDistance * 0.25));
                }
            }
        }

        return maxEDDistance;

    }

   /* StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.minutes(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build(); */

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple20<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>> descriptor =
                new ValueStateDescriptor<>(
                        "maxDistance", // the state name
                        TypeInformation.of(new TypeHint<Tuple20<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>>() {
                        }), // type information
                        new Tuple20(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, this.definedMaxDistance, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, this.definedMaxEuDistance)); // default value of the state, if nothing was set
        distanceThresholds = getRuntimeContext().getState(descriptor);
        // descriptor.enableTimeToLive(ttlConfig);
    }

}