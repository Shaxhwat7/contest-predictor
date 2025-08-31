from functools import lru_cache
from typing import Final
import numpy as np 
from numba import jit

@lru_cache
def pre_sum_sigma(k:int)->float:
    if k<0:
        raise ValueError(f"{k=}, pre_sum index must be non-negative")
    return (5/7)**k + pre_sum_sigma(k-1) if k>=1 else 1

@lru_cache
def delta_adjustment_coefficient(k:int)->float:
    return 1/(1+pre_sum_sigma(k)) if k<=100 else 2/9

def compute_delta_coefficients(ks:np.ndarray)->np.ndarray:
    vectorized_func = np.vectorize(delta_adjustment_coefficient)
    return vectorized_func(ks)

@jit(nopython=True, fastmath=True, parallel=True)
def expected_win_rate(ratings:np.ndarray, opponent_rating:float)->np.ndarray:
    return 1/(1+np.power(10, (opponent_rating-ratings)/400))

@jit(nopython=True, fastmath = True, parallel=True)
def binary_search_expected_ratings(mean_rank:int, all_ratings:np.ndarray)->float:
    target = mean_rank-1
    lo,hi = 0, 400
    max_iter = 25
    precision : Final[float] = 0.01
    while hi-lo>precision and max_iter>=0:
        mid = lo+(hi-lo)/2
        if np.sum(expected_win_rate(all_ratings,mid))<target:
            hi=mid
        else:
            lo=mid
        max_iter-=1
    return mid

@jit(nopython=True, fastmath = True, parallel=True)
def get_expected_rating(rank:int, rating:float, all_ratings:np.ndarray)->float:
    expected_rank = np.sum(expected_win_rate(all_ratings, rating))+0.5
    mean_rank=np.sqrt(expected_rank*rank)
    return binary_search_expected_ratings(mean_rank, all_ratings)

def elo_delta(ranks:np.ndarray, ratings:np.ndarray, ks:np.ndarray)->np.ndarray:
    expected_ratings = np.array(
        [get_expected_rating(ranks[i], ratings[i], ratings) for i in range(len(ranks))]
    )
    return (expected_ratings-ratings)*compute_delta_coefficients(ks)