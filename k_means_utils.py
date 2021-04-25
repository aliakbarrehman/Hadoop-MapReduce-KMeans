import numpy as np
import pandas as pd
import random

def read_centroids(filename):
    centroids_dataframe = pd.read_csv(filepath_or_buffer = filename, sep = ",")
    select_columns = centroids_dataframe.columns[1:]
    x = centroids_dataframe[select_columns]
    return x.to_numpy()

def format_centroids(centroids):
    counter = 1
    result = 'result'
    for i in range(0, centroids.shape[1]):
        result += ',c-' + str(i)
    result += '\n'
    for i in range(0, centroids.shape[0]):
        result += str(counter)
        for j in range(0, centroids.shape[1]):
            result += ',' + str(centroids[i, j])
        result += '\n'
        counter += 1
    return result

def write_centroids(filename, centroids):
    file = open(filename, "w+")
    file.write(format_centroids(centroids))

def create_random_centroids(k, no_of_features):
    result = np.zeros([k, no_of_features], float)
    for i in range(0, k):
        for j in range(0, no_of_features):
            result[i, j] = random.random()
    return result

def get_euclidean_distance(point_a, point_b):
    result = 0
    point_b = point_b.tolist()
    for i in range(0, len(point_a)):
        result += ((point_a[i] - point_b[i]) ** 2)
    return np.sqrt(result)

def read_reducer_output(job, runner):
    centroids = []
    for key, value in job.parse_output(runner.cat_output()):
        centroids.append(value)
    return centroids
    
def compare_centroids(cent_a, cent_b):
    max_dist = 0.0
    for i in range(len(cent_a)):
        dist = get_euclidean_distance(cent_a[i], cent_b[i])
        if dist > max_dist:
            max_dist = dist
    return max_dist