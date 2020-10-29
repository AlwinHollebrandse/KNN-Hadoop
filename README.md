# HadoopKNN
A KNN classifier written with Hadoop.
 
## Running:
A terminal with Hadoop installed is required to run this code. Provided that requirement is met, run `mvn package` in the terminal within the correct maven directory.
`hadoop jar <Jar file maven made> <main file> <training dataset> <output folder> <test dataset> <K>`.
 
### Sample Input:
`hadoop jar target/mapreduce-1.0.jar mapreduce.KNN mediumTrain.txt output mediumTrain.txt 5`.
 
### Sample Output:
Files within the specified output folder that will have a prediction for each test point in the following format (where the left side is the actual value and the right value is the predicted value):
6	6
6	5
6	6
...etc
Along the traditional Hadoop output, this program will print something like:
The KNN classifier for 4898.0 instances required 139528 ms CPU time, accuracy was 0.6565945283789302.
 
## Implementation Details:
This implementation is a mix between the paper found [here](https://sci2s.ugr.es/sites/default/files/files/TematicWebSites/BigData/A%20MapReduce-based%20k-Nearest%20Neighbor%20Approach%20for%20Big%20Data%20Classification-IEEE%20BigDataSE-2015.pdf) and the inspiration code found [here](https://github.com/matt-hicks/MapReduce-KNN/blob/master/KnnPattern.java). This code works by first setting up the configuration of the KNN job by setting the test data and the `k` as configuration variables. Hadoop would then call the mapper. This mapper first runs it's `setup` function which reads the test data and `k` value from the job configuration. This was done so that every mapper would have access to the test data for its own chunk of the train data as described in the inspiration paper. Once the setup was completed the map function would run. This function would find the distance of every one of its Hadoop-determined train data points to every test data point. These distances were then put into a TreeMap that used the computed distances as keys and the class as a value. The smallest `k` key value pairs were kept in the tree. This tree would then be iterated over and have its pairs written to the contest with the index of the current test data point used as the context key. The reducer would then run by first performing its setup method that once again read all the needed variables from the job configuration. The reduce function then combines all the local KNN pairs from each mapper into a global nearest k neighbor pair. The correct local KNNs were found via the test instance index key that mapper used to write to the context. The cleanup method in `KNNReducer` would then perform the majority voting for every test point and would write the final prediction to the context which Hadoop would then write to the specified output location. The final step was to report the CPU time needed for the classifier to run. The CPU time is computed by using "System.nanoTime()" functionality before and after the kNN call.
 
## Results:
The following times were computed by averaging the run CPU times (in ms) for each of the provided dataset 3 times with a k of value 5. It should be noted that for this experiment, the test dataset was identical to the training dataset. This was done to match the previous KNN experiments. However, this code has no method to ignore a distance if the instance of the training and test class were identical. This resulted in the closest neighbor for each test point being itself. However, this would never be an issue with a proper train test data split.
 
| dataset: | Time (ms) | Accuracy |
| --- | --- | --- |
| small | 1512 | 0.869047619047619 |
| medium | 151589 | 0.65659452837893 |
| large | Too long | should be the same as the other KNNs |
| KEEL | Too Long | ? |

 ## Future Work
 Making this implementation faster is the current main future goal. There is a branch "lessContextWrites" on this github that attempted to increase the speedup by changing what was written to the context for each mapper. The other version in question had each mapper write tot eh context once instead of k times like this version does. However, it ended up being slower than the master version of the code.