from mrjob.job import MRJob
import mrjob
from k_means_utils import read_centroids
from k_means_utils import get_euclidean_distance
import numpy as np

class MrJobKMeans(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
    def configure_args(self):
        super(MrJobKMeans, self).configure_args()
        self.add_file_arg('--centroids')

    def read_centroids(filename):
        centroids_dataframe = pd.read_csv(filepath_or_buffer = filename, sep = ",")
        select_columns = centroids_dataframe.columns[1:]
        x = centroids_dataframe[select_columns]
        return x.to_numpy()
        
    def get_euclidean_distance(point_a, point_b):
        result = 0
        point_b = point_b.tolist()
        for i in range(0, len(point_a)):
            result += ((point_a[i] - point_b[i]) ** 2)
        return np.sqrt(result)
        
    def mapper(self, _, line):
        centroids = read_centroids(self.options.centroids)
        coordinates = line.split(',')
        x = []
        index = 0
        for i in coordinates:
            if index != 0:
                x.append(float(i))
                index += 1
        index = 0
        index = 0
        minDist = 10000
        for i in range(1, centroids.shape[0]):
            dist = get_euclidean_distance(x, centroids[i, :])
            if (dist < minDist):
                index = i
                minDist = dist
        yield str(index), line

    def reducer(self, key, values):
        values_list = list(values)
        abc = values_list[0].split(',')
        centroid = np.zeros([1, len(abc)])
        counter = 0
        for i in values_list:
            temp = i.split(',')
            index = 0
            for j in range(0, len(temp)):
                if index != 0:
                    centroid[0, j] += float(temp[j])
                    index += 1
            counter += 1
        centroid = centroid / counter
        temp = ''
        for i in range(0, centroid.shape[1]):
            if (i + 1) == centroid.shape[1]:
                temp += str(centroid[0, i])
            else:
                temp += str(centroid[0, i]) + ','
#        centroid = str(mx) + ',' + str(my)
        yield str(key), temp

if __name__ == '__main__':
    MrJobKMeans.run()