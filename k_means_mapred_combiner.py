from mrjob.job import MRJob
import mrjob
from k_means_utils import read_centroids
from k_means_utils import get_euclidean_distance
import numpy as np

class MrJobKMeansComb(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
    def configure_args(self):
        super(MrJobKMeansComb, self).configure_args()
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
        minDist = 10000
        for i in range(1, centroids.shape[0]):
            dist = get_euclidean_distance(x, centroids[i, :])
            if (dist < minDist):
                index = i
                minDist = dist
        yield str(index), [line.split(','), 1]
        
    def combiner(self, key, values):
        values_list = list(values)
        partial_centroid = np.zeros([1, len(values_list[0])])
        partial_num_points = 0
        for i in values_list:
            partial_num_points += i[1]
            y = list(i[0])
            index = 0
            for k in range(0, len(y)):
                if index != 0:
                    partial_centroid[0, k] += float(y[j])
                    index += 1
        yield str(key), [partial_centroid.tolist(), partial_num_points]

    def reducer(self, key, values):
        values_list = list(values)
        total_num_points = 0
        total_centroid = np.zeros([1, len(values_list[0])])
        for i in values_list:
            total_num_points += i[1]
            y = list(i[0])
            index = 0
            for k in range(0, len(y)):
                if index != 0:
                    total_centroid[0, k] += float(y[j])
                    index += 1
        total_centroid = total_centroid / total_num_points
        temp = ''
        for i in range(0, total_centroid.shape[1]):
            if (i + 1) == total_centroid.shape[1]:
                temp += str(total_centroid[0, i])
            else:
                temp += str(total_centroid[0, i]) + ','
        yield str(key), temp

if __name__ == '__main__':
    MrJobKMeansComb.run()