from mrjob.job import MRJob
from k_means_mapred import MrJobKMeans
from k_means_utils import *
import sys

if __name__ == '__main__':
    args = sys.argv[1:]
    centroidFileName = 'centroids.csv'
    centroidsNumber = 3
    # creates random centroids
    oldCentroids = create_random_centroids(centroidsNumber, 3)
    write_centroids(centroidFileName, oldCentroids)
    iteration = 0
    while True:
        print('Iteration' + str(iteration))
        mr_job = MrJobKMeans(args)
        with mr_job.make_runner() as runner:
            runner.run()
            newCentroids = []
            for key, value in mr_job.parse_output(runner.cat_output()):
                if value is not None:
                    coordinates = value.split(',')
                    temp = []
                    for i in coordinates:
                        temp.append(float(i))
                    newCentroids.append(temp)
            newCentroids = np.array(newCentroids)
            write_centroids(centroidFileName, newCentroids)
        print 'K_Means.py'
        print oldCentroids
        print newCentroids
        temp = diff(newCentroids, oldCentroids)
        print temp
        if temp < 0.00001:
            print('Algorithm Converged!')
            break
        else:
            oldCentroids = newCentroids
        iteration += 1