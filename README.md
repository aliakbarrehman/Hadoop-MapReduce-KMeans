# Hadoop-MapReduce-KMeans

### Run this command to run the K-Means
python 0 k_means.py data.csv --centroids centroids.csv
### Run this command to run the K-Means with Combiner
python 1 k_means.py data.csv --centroids centroids.csv

### To debug mapreduce job run only the mapreduce
python k_means_mapred.py data.csv --centroids centroids.csv
### To debug mapreduce job run only the mapreduce with combiner
python k_means_mapred_combiner.py data.csv --centroids centroids.csv
