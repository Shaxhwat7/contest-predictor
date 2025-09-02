# app/crawler/user.py
from typing import Tuple

from loguru import logger

from core.crawler.utils import multi_http_request
from core.db.models import DATA_REGION


async def request_user_rating_and_attended_contests_count(
    data_region: DATA_REGION,
    username: str,
) -> Tuple[float | None, int | None]:

    if data_region == "CN":
        url = "https://leetcode.cn/graphql/noj-go/"
        query = """
            query userContestRankingInfo($userSlug: String!) {
                userContestRanking(userSlug: $userSlug) {
                    attendedContestsCount
                    rating
                }
            }
        """
        variables = {"userSlug": username}
    else:
        url = "https://leetcode.com/graphql/"
        query = """
            query getContestRankingData($username: String!) {
                userContestRanking(username: $username) {
                    attendedContestsCount
                    rating
                }
            }
        """
        variables = {"username": username}

    try:
        req = (
            await multi_http_request(
                {
                    (data_region, username): {
                        "url": url,
                        "method": "POST",
                        "json": {"query": query, "variables": variables},
                    }
                }
            )
        )[0]

        if req is None:
            raise RuntimeError(f"HTTP request failed for {data_region=} {username=}")

        graphql_res = req.json().get("data", {}).get(
            "userContestRanking", None
        )

        if graphql_res is None:
            return None, None

        return graphql_res.get("rating"), graphql_res.get("attendedContestsCount")

    except Exception as e:
        logger.error(f"Failed to fetch user data for {data_region=} {username=}: {e}")
        return None, None
