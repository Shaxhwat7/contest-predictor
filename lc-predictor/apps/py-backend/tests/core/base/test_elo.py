import numpy as np
import pytest
import sys
import os

# Add the py-backend directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'apps', 'py-backend'))

from core.base.elo import elo_delta
from tests.utils import RATING_DELTA_PRECISION, read_data_contest_prediction_first


@pytest.fixture
def data_contest_prediction_first():
    return read_data_contest_prediction_first()


def test_elo_delta(data_contest_prediction_first):
    """
    Test function for the elo_delta function.

     Raises:
         AssertionError: If not all errors are within the specified precision.
    """

    ks, ranks, old_ratings, new_ratings = data_contest_prediction_first

    delta_ratings = elo_delta(ranks, old_ratings, ks)
    testing_new_ratings = old_ratings + delta_ratings

    errors = np.abs(new_ratings - testing_new_ratings)
    assert np.all(
        errors < RATING_DELTA_PRECISION
    ), f"Elo delta test failed. Some errors are not within {RATING_DELTA_PRECISION=}."
