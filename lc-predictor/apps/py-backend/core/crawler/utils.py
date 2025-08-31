# app/crawler/utils.py
import asyncio
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Union

import httpx
from loguru import logger

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) "
        "Gecko/20100101 Firefox/42.0"
    )
}


async def multi_http_request(
    multi_requests: Dict[Any, Dict],
    concurrent_num: int = 5,
    retry_num: int = 10,
) -> List[Optional[httpx.Response]]:
    """
    Send multiple HTTP requests concurrently with retry and speed control.
    Failed requests return None.
    """
    response_mapper: Dict[Any, Union[int, httpx.Response]] = defaultdict(int)
    crawler_queue = deque(multi_requests.items())
    total_num = len(crawler_queue)
    wait_time = 0

    while crawler_queue:
        requests_list = []

        while len(requests_list) < concurrent_num and crawler_queue:
            key, request = crawler_queue.popleft()
            if response_mapper[key] >= retry_num:
                logger.error(f"Max retries reached for {key=}, {request=}")
                continue
            requests_list.append((key, request))

        if not requests_list:
            break

        logger.info(
            f"remaining={len(crawler_queue) / total_num * 100 :.2f}% "
            f"wait_time={wait_time} "
            f"requests_list={[(key, response_mapper[key]) for key, _ in requests_list]}"
        )

        await asyncio.sleep(wait_time)

        async with httpx.AsyncClient(headers=HEADERS) as client:
            tasks = [client.request(**request) for _, request in requests_list]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            wait_time = 0
            for response, (key, request) in zip(responses, requests_list):
                if isinstance(response, httpx.Response) and response.status_code == 200:
                    response_mapper[key] = response
                else:
                    logger.warning(
                        f"Request failed: {request=} "
                        f"status: {response.status_code if isinstance(response, httpx.Response) else response}"
                    )
                    response_mapper[key] += 1
                    wait_time += 1
                    crawler_queue.append((key, request))

    return [None if isinstance(r, int) else r for r in response_mapper.values()]
