# app/crawler/question.py
from typing import Dict, List, Optional

from loguru import logger

from core.crawler.utils import multi_http_request
from core.db.models import DATA_REGION


async def request_question_list(
    contest_name: str,
    data_region: DATA_REGION = "CN",
) -> Optional[List[Dict]]:

    if data_region == "US":
        url = f"https://leetcode.com/contest/api/info/{contest_name}/"
    elif data_region == "CN":
        url = f"https://leetcode.cn/contest/api/info/{contest_name}/"
    else:
        raise ValueError(f"Unsupported data_region: {data_region}")

    try:
        response = (await multi_http_request({"req": {"url": url, "method": "GET"}}))[0]
        data = response.json()
        question_list = data.get("questions", [])
        
        if data_region == "CN":
            for question in question_list:
                question["title"] = question.get("english_title", question.get("title"))
        
        return question_list

    except Exception as e:
        logger.error(f"Failed to fetch question list for {contest_name=} {data_region=}: {e}")
        return None
