from typing import Final

import numpy as np
from scipy.signal import fftconvolve

from elo import elo_delta

EXPAND_SIZE: Final[int] = 100
MAX_RATING: Final[int] = 4000 * EXPAND_SIZE


def pre_calc_convolution(old_ratings: np.ndarray) -> np.ndarray:
    f = 1 / (1 + np.power(10, np.arange(-MAX_RATING, MAX_RATING + 1) / (400 * EXPAND_SIZE)))
    g = np.bincount(np.round(old_ratings * EXPAND_SIZE).astype(int))
    conv = fftconvolve(f, g, mode="full")
    return conv[: 2 * MAX_RATING + 1]


def get_expected_rank(conv: np.ndarray, x: int) -> float:
    return conv[x + MAX_RATING] + 0.5


def get_equation_left(conv: np.ndarray, x: int) -> float:
    return conv[x + MAX_RATING] + 1


def binary_search_expected_rating(conv: np.ndarray, mean_rank: float) -> int:
    lo, hi = 0, MAX_RATING
    while lo < hi:
        mid = (lo + hi) // 2
        if get_equation_left(conv, mid) < mean_rank:
            hi = mid
        else:
            lo = mid + 1
    return mid


def get_expected_rating(rank: int, rating: float, conv: np.ndarray) -> float:
    scaled_rating = round(rating * EXPAND_SIZE)
    expected_rank = get_expected_rank(conv, scaled_rating)
    mean_rank = np.sqrt(expected_rank * rank)
    return binary_search_expected_rating(conv, mean_rank) / EXPAND_SIZE


def fft_delta(ranks: np.ndarray, ratings: np.ndarray, ks: np.ndarray) -> np.ndarray:
    conv = pre_calc_convolution(ratings)
    expected_ratings = np.array([get_expected_rating(rank, rating, conv)
                                 for rank, rating in zip(ranks, ratings)])
    return (expected_ratings - ratings) * elo_delta(ks)
