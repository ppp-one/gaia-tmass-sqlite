import numpy as np
from multiprocessing import Pool
from single_query import single_query


if __name__ == "__main__":

    arr = np.arange(-90,90.01, 0.01)

    with Pool() as p:
        p.starmap(single_query, [(arr[i], arr[i+1]) for i in range(len(arr)-1)])
