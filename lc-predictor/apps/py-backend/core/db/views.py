from pydantic import BaseModel

from db.models import DATA_REGION


class UserKey(BaseModel):
    username: str
    data_region: DATA_REGION
