from datetime import datetime
from typing import Final, NamedTuple


NEW_USER_CONTESTS_ATTENDED: Final[int] = 0
NEW_USER_INITIAL_RATING: Final[float] = 1500.0

class ContestSchedule(NamedTuple):
    weekday: int 
    hour: int
    minute: int


WEEKLY_CONTEST_TIME = ContestSchedule(
    weekday=6,  
    hour=2,
    minute=30,
)
BIWEEKLY_CONTEST_TIME = ContestSchedule(
    weekday=5,  
    hour=14,
    minute=30,
)

class ContestReference(NamedTuple):
    number: int
    start_dt: datetime


WEEKLY_CONTEST_REF = ContestReference(
    number=294,
    start_dt=datetime(2022, 5, 22, 2, 30),
)

BIWEEKLY_CONTEST_REF = ContestReference(
    number=78,
    start_dt=datetime(2022, 5, 14, 14, 30),
)
