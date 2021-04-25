from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import random
from k_means_utils import format_centroids
from k_means_utils import write_centroids
import timeit


start_time = timeit.default_timer()
spark_session = SparkSession.builder.appName('k_means').getOrCreate()
data = spark_session.read.csv('expanded_data_cleaned_reduced.csv', header=True, inferSchema=True)

cols = data.columns
cols = cols[1:]

assembler = VectorAssembler(inputCols = cols, outputCol = 'features')
assembled_data = assembler.transform(data)

seed = int(random.random() * 1000 + 10)
k_means = KMeans(k = 5, seed = seed)  # 2 clusters here
model = k_means.fit(assembled_data.select('features'))
print model.clusterCenters()

time_taken = timeit.default_timer() - start_time
print time_taken

write_centroids(model.clusterCenters())