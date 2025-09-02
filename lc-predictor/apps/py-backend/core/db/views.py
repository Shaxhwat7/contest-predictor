from pydantic import BaseModel

from core.db.models import DATA_REGION


class UserKey(BaseModel):
    username: str
    data_region: DATA_REGION
