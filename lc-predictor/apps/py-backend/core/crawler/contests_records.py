# app/crawler/contest_records.py
from math import ceil
from typing import Dict, Final, List, Tuple

from loguru import logger

from crawler.utils import multi_http_request
from db.models import DATA_REGION


async def request_contest_records(
    contest_name: str,
    data_region: DATA_REGION,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Fetch all ranking records of a contest by sending HTTP requests per page concurrently.

    :param contest_name: Contest slug/title.
    :param data_region: Either 'US' or 'CN'.
    :return: Tuple containing
             1. List of contest records.
             2. List of nested submission records.
    """
    base_url: Final[str] = "https://leetcode.com" if data_region == "US" else "https://leetcode.cn"
    logger.info(f"Fetching contest records from {base_url=}")

    try:
        first_page_response = (
            await multi_http_request(
                {"req": {"url": f"{base_url}/contest/api/ranking/{contest_name}/", "method": "GET"}}
            )
        )[0]
        data = first_page_response.json()
    except Exception as e:
        logger.error(f"Failed to fetch first page for {contest_name=} {data_region=}: {e}")
        return [], []

    user_num = data.get("user_num", 0)
    page_max = ceil(user_num / 25)
    logger.info(f"{user_num=} participants, {page_max=} pages")

    contest_record_list: List[Dict] = []
    nested_submission_list: List[Dict] = []

    url_list = [
        f"{base_url}/contest/api/ranking/{contest_name}/?pagination={page}&region=global"
        for page in range(1, page_max + 1)
    ]

    try:
        responses = await multi_http_request(
            {url: {"url": url, "method": "GET"} for url in url_list},
            concurrent_num=5 if data_region == "US" else 10,
        )
    except Exception as e:
        logger.error(f"Failed to fetch pages concurrently for {contest_name=} {data_region=}: {e}")
        return [], []

    for res in responses:
        if not res:
            continue
        try:
            res_dict = res.json()
            contest_record_list.extend(res_dict.get("total_rank", []))
            nested_submission_list.extend(res_dict.get("submissions", []))
        except Exception as e:
            logger.warning(f"Failed to parse response page: {e}")

    logger.success(f"Finished fetching contest records for {contest_name=}")
    return contest_record_list, nested_submission_list
