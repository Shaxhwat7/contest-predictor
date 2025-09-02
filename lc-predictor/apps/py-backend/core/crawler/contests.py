# app/crawler/contest.py
import re
from typing import Dict, List, Optional, Final

from loguru import logger

from core.crawler.utils import multi_http_request
from core.db.models import DATA_REGION


async def request_contest_user_num(contest_name: str, data_region: DATA_REGION) -> Optional[int]:

    url: Final[str] = (
        f"https://leetcode.com/contest/api/ranking/{contest_name}/?region=us"
        if data_region == "US"
        else f"https://leetcode.cn/contest/api/ranking/{contest_name}/?region=cn"
    )
    try:
        response = (await multi_http_request({"req": {"url": url, "method": "GET"}}))[0]
        return response.json().get("user_num")
    except Exception as e:
        logger.error(f"Failed to fetch user_num for {contest_name=} {data_region=}: {e}")
        return None


async def request_past_contests(max_page_num: int) -> List[Dict]:

    requests = {
        page_num: {
            "url": "https://leetcode.com/graphql/",
            "method": "POST",
            "json": {
                "query": """
                    query pastContests($pageNo: Int) {
                        pastContests(pageNo: $pageNo) {
                            data { title titleSlug startTime duration }
                        }
                    }
                """,
                "variables": {"pageNo": page_num},
            },
        }
        for page_num in range(1, max_page_num + 1)
    }

    responses = await multi_http_request(requests, concurrent_num=10)
    past_contests: List[Dict] = []

    for response in responses:
        try:
            past_contests.extend(
                response.json().get("data", {}).get("pastContests", {}).get("data", [])
            )
        except Exception as e:
            logger.warning(f"Failed to parse past contests page: {e}")

    logger.info(f"Fetched {len(past_contests)} past contests across {max_page_num} pages")
    return past_contests


async def request_contest_homepage_text() -> str:

    try:
        response = (await multi_http_request({"req": {"url": "https://leetcode.com/contest/", "method": "GET"}}))[0]
        return response.text
    except Exception as e:
        logger.error(f"Failed to fetch contest homepage: {e}")
        return ""


async def request_next_two_contests() -> List[Dict]:

    homepage_text = await request_contest_homepage_text()
    build_id_match = re.search(r'"buildId":\s*"(.*?)"', homepage_text)
    if not build_id_match:
        logger.error("Cannot find buildId in homepage HTML")
        return []

    build_id = build_id_match.group(1)
    try:
        response = (await multi_http_request(
            {"req": {"url": f"https://leetcode.com/_next/data/{build_id}/contest.json", "method": "GET"}}
        ))[0]
        data = response.json()
    except Exception as e:
        logger.error(f"Failed to fetch next two contests data: {e}")
        return []

    queries = data.get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
    for query in queries:
        top_two = query.get("state", {}).get("data", {}).get("topTwoContests")
        if top_two:
            logger.info(f"Next two contests: {top_two}")
            return top_two

    logger.error("Cannot find topTwoContests in response")
    return []


async def request_all_past_contests() -> List[Dict]:

    homepage_text = await request_contest_homepage_text()
    page_num_match = re.search(r'"pageNum":\s*(\d+)', homepage_text)
    if not page_num_match:
        logger.error("Cannot find pageNum in homepage HTML")
        return []

    max_page_num = int(page_num_match.group(1))
    return await request_past_contests(max_page_num)


async def request_recent_contests() -> List[Dict]:

    return await request_past_contests(1)
