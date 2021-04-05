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

    def mapper(self, _, line):
        centroids = read_centroids(self.options.centroids)
        coordinates = line.split(',')
        x = []
        for i in coordinates:
            x.append(float(i))
        index = 0
        minDist = 10000
        for i in range(0, centroids.shape[0]):
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
            for j in range(0, len(temp)):
                centroid[0, j] += float(temp[j])
            counter += 1
        centroid = centroid / counter
        temp = ''
        for i in range(0, centroid.shape[1]):
            if (i + 1) == centroid.shape[1]:
                temp += str(centroid[0, i])
            else:
                temp += str(centroid[0, i]) + ','
        # centroid = str(mx) + ',' + str(my)
        yield str(key), temp

if __name__ == '__main__':
    MrJobKMeans.run()