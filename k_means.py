from mrjob.job import MRJob
from k_means_mapred import MrJobKMeans
from k_means_utils import *
import sys
import timeit

if __name__ == '__main__':
    args = sys.argv[1:]
    centroidFileName = 'centroids.csv'
    centroidsNumber = 3
    # creates random centroids
    oldCentroids = create_random_centroids(centroidsNumber, 5)
    write_centroids(centroidFileName, oldCentroids)
    iteration = 0
    start_time = timeit.default_timer()
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
        cent_diff = compare_centroids(newCentroids, oldCentroids)
        if cent_diff < 0.01:
            print('Algorithm Converged!')
            break
        else:
            oldCentroids = newCentroids
        iteration += 1
    time_taken = timeit.default_timer() - start_time
    print time_taken