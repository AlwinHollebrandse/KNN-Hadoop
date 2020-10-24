# HadoopKNN
A KNN classifier written with Hadoop.
 
## Running:
A terminal with Hadoop installed is required to run this code. Provided that requirement is met, run `mvn package` in the terminal within the correct maven directory.
`hadoop jar <Jar file maven made> <main file> <training dataset> <output folder> <test dataset> <K>`.
 
### Sample Input:
`hadoop jar target/mapreduce-1.0.jar mapreduce.KNN mediumTrain.txt output mediumTrain.txt 5`.
 
### Sample Output:
Files within the specified output folder that will have a prediction for each test point in the following format:
Test Index: 0, predictedClass:  6
Test Index: 1, predictedClass:  5
...etc
 
## Implementation Details:
This implementation is a mix between the paper found [here](https://sci2s.ugr.es/sites/default/files/files/TematicWebSites/BigData/A%20MapReduce-based%20k-Nearest%20Neighbor%20Approach%20for%20Big%20Data%20Classification-IEEE%20BigDataSE-2015.pdf) and the inspiration code found [here](https://github.com/matt-hicks/MapReduce-KNN/blob/master/KnnPattern.java). This code works by first setting up the configuration of the KNN job by setting the test data and the `k` as configuration variables. Hadoop would then call the mapper. This mapper first runs it's `setup` function which reads the test data and `k` value from the job configuration. This was done so that every mapper would have access to the test data for its own chunk of the train data as described in the inspiration paper. Once the setup was completed the map function would run. This function would find the distance of every one of its Hadoop-determined train data points to every test data point. These distances were then put into a TreeMap that used the computed distances as keys and the class as a value. The smallest `k` key value pairs were kept in the tree. This tree would then be iterated over and have its pairs written to the contest with the index of the current test data point used as the context key. The reducer would then run by first performing its setup method that once again read all the needed variables from the job configuration. The reduce function then combines all the local KNN pairs from each mapper into a global nearest k neighbor pair. The correct local KNNs were found via the test instance index key that mapper used to write to the context. The cleanup method in `KNNReducer` would then perform the majority voting for every test point and would write the final prediction to the context which Hadoop would then write to the specified output location. The final step was to report the CPU time needed for the classifier to run. The CPU time is computed by using "clock_gettime" functionality before and after the kNN call.
 
## kNN:
The kNN call (getKNNForInstance) is computed as follows: for a given dataset instance, calculate the euclidean distance compared to each other dataset instances. Record these distances and the attached class of each value. Sort this resulting array in ascending order according to distance. Take the first `k` pairs of the sorted list and perform `kVoting` to get a prediction. Voting works but getting the count of each class in the kNN values. The class with the most votes is predicted. In the event of a tie, the first class encountered that had that vote amount is returned.
 
## Results:
The following times were computed by averaging the run CPU times (in ms) for each of the provided dataset 3 times with a k of value 5. It should be noted that for this experiment, the test dataset was identical to the training dataset. This was done to match the previous KNN experiments. However, this code has no method to ignore a distance if the instance of the training and test class were identical. This resulted in the closest neighbor for each test point being itself. However, this would never be an issue with a proper train test data split.
 
| -np: | large | medium | small |
| --- | --- | --- | --- |
| 1 | 169452.3333 | 10587.66667 | 266 |
| 2 | 88465.33333 | 5428 | 256.6666667 |
| 4 | 19592 | 2941.333333 | 263.3333333 |
| 8 | TODO | 1678.666667 | 267 |
| 16 | 13622.33333 | 1081 | 295.3333333 |
 