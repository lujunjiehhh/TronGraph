"""
trx_address.py

该模块提供了 TrxAddress 类，用于获取 TRX 地址的相关交易数据。
通过异步请求方式与 Tron 链交互，并使用限速器控制请求速率以避免被封禁。
支持多种 API 平台（如 trongrid、tronscan 和 oklink），可以根据需要选择使用。

类 TrxAddress:
    初始化时，接收 TRX 地址、API 密钥列表和可选时间戳和代理地址Args。
    提供获取首笔 TRX 转出地址及交易 ID、TRC20 转账数量、TRX 转账数量、
    起始和结束时间以及特定时间段内的交易数据等功能。

函数 fetch_data:
    内部实现，用于执行异步 HTTP GET 请求并处理响应。根据Args动态选择 API 平台和 URL 模板。

辅助类 RateLimiter 和 :
    用于限速器，确保代码可读性和易于维护。
"""

import asyncio
import json
import math
import os
import random
import time
from functools import reduce
from typing import Dict

import aiohttp
import base58
import pandas as pd
from aiomultiprocess import Pool
from loguru import logger
from tqdm import tqdm

from utils import TimePeriod, RateLimiter, RateLimiteException, TokenProducer


async def fetch_trans_data_for_period(
    address,
    start_timestamp,
    end_timestamp,
    token_id,
    is_trx,
    only_form,
    only_to,
    api_keys,
    platform,
    proxy,
    require_count,
):
    """
    放在类的外面，防止pickle的拷贝问题
    :param address:
    :type address:
    :param start_timestamp:
    :type start_timestamp:
    :param end_timestamp:
    :type end_timestamp:
    :param token_id:
    :type token_id:
    :param is_trx:
    :type is_trx:
    :param only_form:
    :type only_form:
    :param only_to:
    :type only_to:
    :param api_keys:
    :type api_keys:
    :param platform:
    :type platform:
    :param proxy:
    :type proxy:
    :param require_count:
    :type require_count:
    :return:
    :rtype:
    """
    rate_limiters = {
        platform_name
        + "---"
        + key: RateLimiter(
            platform["key_rate"] - 1,
            1,
            platform_name + "---" + key,
        )
        for platform_name, platform in platform.items()
        for key in api_keys[platform_name]
    }
    trx_address = TrxAddress(
        api_keys=api_keys,
        platform=platform,
        proxy=proxy,
        rate_limiters=rate_limiters,
        segments_num=require_count,
    )
    data, count = await trx_address.get_token_trade(
        address,
        is_trx,
        start_timestamp,
        end_timestamp,
        token_id,
        only_form,
        only_to,
    )
    # await trx_address.session.close()  # 确保关闭 session
    return data, count


async def fetch_groupp_data_for_period(
    address, start_timestamp, end_timestamp, query_type, api_keys, platform, proxy
):
    """
    放在类的外面，防止pickle的拷贝问题
    :param address:
    :type address:
    :param start_timestamp:
    :type start_timestamp:
    :param end_timestamp:
    :type end_timestamp:
    :param query_type:
    :type query_type:
    :param api_keys:
    :type api_keys:
    :param platform:
    :type platform:
    :param proxy:
    :type proxy:
    :return:
    :rtype:
    """
    rate_limiters = {
        platform_name
        + "---"
        + key: RateLimiter(
            platform["key_rate"] - 1,
            1,
            platform_name + "---" + key,
        )
        for platform_name, platform in platform.items()
        for key in api_keys[platform_name]
    }
    trx_address = TrxAddress(
        api_keys=api_keys, platform=platform, proxy=proxy, rate_limiters=rate_limiters
    )
    data = await trx_address.daily_analytics(address, start_timestamp, end_timestamp, query_type)
    return data["data"]


class TrxAddress:
    """
    TrxAddress类用于处理TRX地址相关的数据。它包含以下属性：

    Attributes:
        timestamp (int): 时间戳。
        address (str): TRX地址。
        next_url (str): 下一次请求的URL。
        curren_url (str): 当前请求的URL。

    Constructor:
        __init__(): 初始化TrxAddress对象。
            - api_keys: API密钥，默认为环境变量中的TRON_API_KEYS。
            - timestamp: 时间戳，默认为0。
            - proxy: 代理服务器，默认为None。

    Methods:
        fetch_data(): 发送HTTP请求获取数据。
            - platform: 平台名称（如"trongrid", "tronscan", "oklink"）。
            - method: 方法名称。
            - method_param: 方法Args。
            - url_param: URLArgs。
            - url: 完整的URL地址，当url不为None时，其他Args无效。

        is_contract_address: 检查给定地址是否为合约地址。

        get_first_trx_from: 获取首笔TRX转出地址和交易ID。

        get_trc20_transfer_quantity: 获取TRC20的转账数量。

        get_trx_transfer_quantity: 获取TRX的转账数量。

        get_start_and_end_time: 根据地址和是否为TRX，获取起始时间和结束时间。

        get_trans_data: 获取指定地址在指定时间段内的交易数据。

        get_token_trade: 查询USDT转出转入记录。

        group_max_time -> pd.DataFrame: 按照from和to地址聚合，取最大时间戳。

        group_min_time -> pd.DataFrame: 按照from和to地址聚合，取最小时间戳。

        get_group_trade -> pd.DataFrame:
            聚合数据并统计每个地址与当前地址的交易次数和交易金额。
    """

    timestamp = 0
    address = ""
    next_url = ""
    curren_url = ""

    def __init__(
        self,
        rate_limiters=None,
        platform=None,
        api_keys=None,
        timestamp=0,
        proxy=None,
        segments_num=8
    ):
        self.segments_num = segments_num
        self.count = 0
        self.timestamp = timestamp
        self.next_url = ""
        self.curren_url = ""
        # 如果环境变量中存在api key，则使用环境变量中的api key，否则使用传入的Args中的api key
        self.proxy = proxy
        self.api_keys = api_keys
        self.platform = platform
        self.platform_baseurl = {
            v["baseUrl"]: platform_name for platform_name, v in self.platform.items()
        }
        self.rate_limiters: Dict[str, RateLimiter] = rate_limiters
        self.session = aiohttp.ClientSession()

    async def daily_analytics(self, address, start_timestamp, end_timestamp, query_type):
        types = {
            "Balance": 0,
            "Transfer": 1,
            "Energy": 2,
            "Bandwidth": 3,
            "Transaction": 4,
        }
        query_type = types.get(query_type, -1)

        if query_type == -1:
            raise ValueError(f"{query_type} is not in the types.")

        url_param = {
            "address": address,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "type": query_type,
        }
        return await self.fetch_data("tronscan", "daily_analytics", url_param=url_param)

    async def fetch_data(
        self, platform=None, method=None, method_param=None, url_param=None, url=None
    ):
        """
        根据指定的平台、方法以及Args获取数据。

        Args:
            platform (str): 平台名称，如果url为None时必填
            method (str): 方法名称，如果url为None时必填
            method_param (dict): 方法Args字典
            url_param (dict): URL查询Args字典
            url (str): 直接提供完整的URL，当platform和method均为None时使用

        Returns:
            json: 获取到的数据内容

        异常:
            ValueError: 当url为None且platform或method未指定时抛出
            ValueError: 当指定的平台或方法不存在API密钥时抛出

        """
        session = self.session
        all_index = list(self.rate_limiters.keys())
        api_keys = await self.rate_limiters[all_index[0]].get_all_usable_apikey(platform)
        api_key = random.choice(api_keys)
        headers = self.platform[platform]["headers"].format(api_key=api_key)
        # 如果提供了URL，则直接使用该URL作为请求地址
        if url is None:
            if platform is None or method is None:
                raise ValueError("When url is None, platform and method must not be None")
            if not self.api_keys[platform]:
                raise ValueError("No API keys available for platform")
            if platform not in self.api_keys.keys() or method not in self.platform[platform].keys():
                raise ValueError(
                    f"No platform or method keys available for platform\n{platform}\n{method}"
                )

            if method_param is None:
                url = self.platform[platform]["baseUrl"] + self.platform[platform][method]
            else:
                url = self.platform[platform]["baseUrl"] + self.platform[platform][method].format(
                    **method_param
                )
        data = "Data request failed"
        min_backoff = 0.125
        for i in range(5):
            platform_key = platform + "---" + api_key
            await self.rate_limiters[platform_key].acquire()  # 在每次请求前获取令牌
            try:
                if self.proxy is None:
                    response = await session.get(
                        url, params=url_param, headers=eval(headers), ssl=False
                    )
                else:
                    response = await session.get(
                        url,
                        params=url_param,
                        headers=eval(headers),
                        proxy=self.proxy,
                        ssl=False,
                    )
                self.count += 1
                data = await response.json()
                if response.status == 200:
                    return data
                tokens = await self.rate_limiters[platform_key].get_tokens()
                logger.warning(f"{platform} {api_key}\t{data}\ncurrent tokens:{tokens}")
                raise RateLimiteException(
                    f"{platform} {api_key} Rate limit exceeded. Please try again later."
                )
            except aiohttp.ClientError as e:
                # 指数退让
                logger.warning(str(e), "Retrying...")
                timeout = min_backoff * (2**i) + random.uniform(0, min_backoff)
                await asyncio.sleep(timeout)
            # 超过api速率限制
            except RateLimiteException as e:
                if i == 0:
                    logger.warning(data, str(e))
                    await self.rate_limiters[platform_key].notify_stop_production(50)
                    api_keys = await self.rate_limiters[platform_key].get_all_usable_apikey(
                        platform
                    )
                    api_key = random.choice(api_keys)
                    headers = self.platform[platform]["headers"].format(api_key=api_key)
                else:
                    raise RateLimiteException(f"All apis of {platform} are unavailable") from e
        logger.error("Network error or server timeout after multiple retries\n")
        raise aiohttp.ClientError("Network error or server timeout after multiple retries\n")

    async def is_contract_address(self, address):
        """
        检查给定的地址是否为合约地址。

        Args:
            address (str): 需要检查的地址

        Returns:
            bool: 如果是合约地址则ReturnsTrue，否则ReturnsFalse。
        """
        result = await self.fetch_data(
            "tronscanapi", "is_contract_address", url_param={"contract": address}
        )
        result = result["data"][0]["address"]
        if result != "":
            return True
        return False

    async def get_first_trx_from(self, address):
        """
        获取首笔trx转出地址和交易id
        :param address: 交易地址
        :type address: str
        :return: 首笔trx转出地址和交易id
        :rtype: tuple
        """
        method = "get_trx_trade"
        fetch_data = await self.fetch_data(
            "trongrid",
            method=method,
            method_param={
                "address": address,
            },
            url_param={
                "limit": 1,
                "order_by": "block_timestamp,asc",
                "min_timestamp": 0,
            },
        )
        hex_address: str = fetch_data["data"][0]["raw_data"]["contract"][0]["parameter"]["value"][
            "owner_address"
        ]

        address = base58.b58encode_check(bytes.fromhex(hex_address)).decode("utf-8")
        tx_id = fetch_data["data"][0]["txID"]
        return address, tx_id

    async def get_trc20_transfer_quantity(
        self, address, token_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    ):
        """
        获取指定地址在特定TRC20代币上的转账数量。

        :param address: 待查询的TRON账户地址
        :param token_address: 要查询的TRC20代币合约地址，默认为TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t
        :return: (转入数量, 转出数量)
        """
        method = "get_trc20_transfer_quantity"
        url_param = {"accountAddress": address, "tokenAddress": token_address}
        response = await self.fetch_data("tronscan", method, url_param=url_param)
        return response["transferIn"], response["transferOut"]

    async def get_trx_transfer_quantity(self, address):
        """
        获取指定地址在TRON主网上的转账数量。

        :param address: 待查询的TRON账户地址
        :return: 转账次数
        """
        method = "get_trx_transfer_quantity"
        url_param = {"chainShortName": "tron", "address": address}
        response = await self.fetch_data("oklink", method, url_param=url_param)
        return int(response["data"][0]["transactionCount"])

    async def get_start_and_end_time(self, address, token_id, is_trx):
        """
        获取指定地址在TRON主网上的转账开始和结束时间。

        :param address: 待查询的TRON账户地址
        :param token_id: 代币地址
        :param is_trx: 是否为TRX主网，True表示TRX，False表示TRC20代币
        :return: (开始时间戳, 结束时间戳)
        """
        if is_trx:
            method = "get_trx_start_and_end_time"
            url_param = {"chainShortName": "tron", "address": address}
            response = await self.fetch_data("oklink", method, url_param=url_param)
            start = int(response["data"][0]["firstTransactionTime"])
            end = int(response["data"][0]["lastTransactionTime"])
        else:
            method = "get_start_and_end_time"
            method_param = {"address": address}
            url_param = {
                "limit": 1,
                "order_by": "block_timestamp,asc",
                "min_timestamp": 0,
                "contract_address": token_id,
            }
            response = await self.fetch_data("trongrid", method, method_param, url_param)
            start = int(response["data"][0]["block_timestamp"])

            url_param = {
                "limit": 1,
                "order_by": "block_timestamp,desc",
                "min_timestamp": 0,
                "contract_address": token_id,
            }
            response = await self.fetch_data("trongrid", method, method_param, url_param)
            end = int(response["data"][0]["block_timestamp"])

        return start, end

    async def get_trans_data(
        self,
        address,
        start_timestamp,
        end_timestamp,
        token_id,
        is_trx,
        only_form,
        only_to,
    ):
        """

        获取指定地址在指定时间段内的交易数据。

        Args:
            address (str): 查询地址
            is_trx (bool): 是否为TRX交易，True表示TRX交易，False表示USDT交易。
            start_timestamp (int): 起始时间戳（毫秒）。
            end_timestamp (int): 结束时间戳（毫秒）。
            only_form (bool, optional): 是否只获取from地址的交易记录，默认为False。
            only_to (bool, optional): 是否只获取to地址的交易记录，默认为False。
            token_id (str, optional): 代币地址
        Returns:
            pd.DataFrame: 包含交易数据的DataFrame。
        """
        trans_data = pd.DataFrame()
        timestamp = start_timestamp
        last_timestamp = start_timestamp
        next_url = ""
        count = 0
        last_len = 0

        while True:
            count += 1
            platform = "trongrid"
            method = "get_usdt_trade" if not is_trx else "get_trx_trade"
            method_param = {"address": address}
            url_param = {
                "limit": 200,
                "order_by": "block_timestamp,asc",
                "min_timestamp": last_timestamp,
                "max_timestamp": end_timestamp,
            }
            if not is_trx:
                url_param["contract_address"] = token_id
            if only_form:
                url_param["only_from"] = True
            elif only_to:
                url_param["only_to"] = True
            data = await self.fetch_data(platform, method, method_param, url_param)

            if not data or len(data["data"]) == 0:
                break

            if len(data["data"]) == last_len and len(data["data"]) < 200:
                break

            last_len = len(data["data"])
            last_timestamp = data["data"][-1]["block_timestamp"]

            if timestamp == last_timestamp:
                if next_url == "":
                    continue
                url = next_url
                data = await self.fetch_data(url=url)
                last_timestamp = data["data"][-1]["block_timestamp"]

            if not is_trx:
                trans_data = trans_data._append(data["data"])
            else:
                for tx in data["data"]:
                    if tx["raw_data"]["contract"][0]["type"] == "TransferContract":
                        from_address = base58.b58encode_check(
                            bytes.fromhex(
                                tx["raw_data"]["contract"][0]["parameter"]["value"]["owner_address"]
                            )
                        ).decode("utf-8")
                        to_address = base58.b58encode_check(
                            bytes.fromhex(
                                tx["raw_data"]["contract"][0]["parameter"]["value"]["to_address"]
                            )
                        ).decode("utf-8")
                        timestamp = tx["block_timestamp"]
                        value = tx["raw_data"]["contract"][0]["parameter"]["value"]["amount"]
                        trans_data = trans_data._append(
                            [
                                {
                                    "block_timestamp": timestamp,
                                    "from": from_address,
                                    "to": to_address,
                                    "value": value,
                                    "transaction_id": tx["txID"],
                                }
                            ]
                        )
            try:
                next_url = data["meta"]["links"]["next"]
            # 如果没有next
            except KeyError:
                next_url = ""
        return trans_data, count

    # trx_address.py (515-559)

    async def get_token_trade(
        self,
        address,
        is_trx,
        start_timestamp,
        end_timestamp,
        token_id=None,
        only_form=False,
        only_to=False,
    ):
        """
        获取代币交易数据

        Args:
            start_timestamp (int): 开始时间
            end_timestamp (int): 结束时间
            address (str): 待查询地址
            is_trx (bool): 是否为TRX代币
            token_id (str): 代币地址
            only_form (bool, optional): 是否只统计发送方交易数量，默认为False
            only_to (bool, optional): 是否只统计接收方交易数量，默认为False

        Returns:
            pandas.DataFrame: 包含代币交易数据的DataFrame

        """

        # # 获取交易时间范围
        # start_timestamp, end_timestamp = await self.get_start_and_end_time(
        #     address=address, token_id=token_id, is_trx=False
        # )
        #
        # # 根据输入Args调整查询时间范围
        # if start_or_end_time != 0:
        #     if only_form:
        #         start_timestamp = start_or_end_time
        #     elif only_to:
        #         end_timestamp = start_or_end_time
        #
        # # 获取交易次数
        # transfer_count = await self.get_transfer_count(
        #     address, is_trx, only_form, only_to, token_id
        # )

        # 根据所需数量计算时间间隔
        require_count = self.segments_num

        # 如果需要处理多个时间区间，则分批获取数据
        if require_count > 1:
            time_intervals = self.create_time_intervals(
                require_count,
                address,
                start_timestamp,
                end_timestamp,
                token_id,
                is_trx,
                only_form,
                only_to,
            )
            trans_data, count = await self._fetch_data_in_intervals(time_intervals)
        else:
            # 获取单个时间段的交易数据
            trans_data, count = await self.get_trans_data(
                address,
                start_timestamp,
                end_timestamp,
                token_id,
                is_trx,
                only_form,
                only_to,
            )

        # 去重处理
        trans_data.drop_duplicates(subset=["transaction_id"], keep="first", inplace=True)
        if not trans_data.empty:
            trans_data["value"] = trans_data["value"].apply(lambda x: int(x) / 1e6)
            trans_data["time"] = trans_data["block_timestamp"].apply(
                lambda x: pd.Timestamp(x / 1000, unit="s", tz="Asia/Shanghai")
            )
            if not is_trx:
                trans_data.drop(["token_info"], axis=1, inplace=True)
        return trans_data, count

    async def get_transfer_count(self, address, is_trx, only_form, only_to, token_id=None):
        """
        获取代币交易次数

        Args:
            address (str): 待查询地址
            is_trx (bool): 是否为TRX代币
            token_id (str)
            only_form (bool, optional): 是否只统计发送方交易数量，默认为False
            only_to (bool, optional): 是否只统计接收方交易数量，默认为False

        Returns:
            int: 代币交易次数
        """

        # 根据是否为TRX和查询方向获取代币交易次数
        if not is_trx:
            if only_to:
                return (await self.get_trc20_transfer_quantity(address, token_id))[0]
            if only_form:
                return (await self.get_trc20_transfer_quantity(address, token_id))[1]
            return await self.get_trc20_transfer_quantity(address, token_id)
        return await self.get_trx_transfer_quantity(address)

    @staticmethod
    def create_time_intervals(
        require_count,
        address,
        start_timestamp,
        end_timestamp,
        token_id,
        is_trx,
        only_form,
        only_to,
    ):
        """
        根据给定的时间范围和要求数量，生成多个时间间隔。

        Args:
            address （str): 查询地址
            start_timestamp (int): 起始时间戳
            end_timestamp (int): 结束时间戳
            token_id (str): 代币地址
            require_count (int): 需要的时间间隔数量
            is_trx (bool): 是否为TRX交易
            only_form (str): 指定来源地址（可选）
            only_to (str): 指定目标地址（可选）

        Returns:
            list: 时间间隔列表，每个时间间隔是一个字典，包含"is_trx"、"start_timestamp"、"end_timestamp"、"only_form"和"only_to"键值对。
        """
        interval = (end_timestamp - start_timestamp) // require_count - 1
        time_intervals = [
            {
                "address": address,
                "token_id": token_id,
                "is_trx": is_trx,
                "start_timestamp": start_timestamp + i * interval,
                "end_timestamp": start_timestamp + (i + 1) * interval,
                "only_form": only_form,
                "only_to": only_to,
            }
            for i in range(require_count - 1)
        ]
        time_intervals.append(
            {
                "address": address,
                "token_id": token_id,
                "is_trx": is_trx,
                "start_timestamp": time_intervals[-1]["end_timestamp"],
                "end_timestamp": end_timestamp,
                "only_form": only_form,
                "only_to": only_to,
            }
        )
        return time_intervals

    async def _fetch_data_in_intervals(self, time_intervals):
        dataframes = []
        count = 0
        for time_interval in tqdm(time_intervals,desc=str(os.getpid())):
            result = await self.get_trans_data(**time_interval)
            dataframes.append(result[0])
            count += result[1]
        return pd.concat(dataframes, ignore_index=True), count

    @staticmethod
    def group_max_time(data: pd.DataFrame) -> pd.DataFrame:
        """
        对输入的DataFrame进行分组，计算每个组的最大transaction_id、value、block_timestamp和time。

        Args:
            data: pd.DataFrame - 包含"from", "to", "transaction_id", "value",
                                        "block_timestamp"和"time"列的DataFrame。

        Returns值:
            pd.DataFrame - 包含"src", "dst", "count", "timestamp"和"amount"列的新DataFrame，
                           其中"src"和"dst"分别代表原始"from"和"to"列的值。
        """
        data = (
            data.pivot_table(
                index=["from", "to"],
                aggfunc={
                    "transaction_id": "count",
                    "value": "sum",
                    "block_timestamp": "max",
                    "time": "max",
                },
            )
            .reset_index()
            .rename(
                columns={
                    "from": "src",
                    "to": "dst",
                    "transaction_id": "count",
                    "block_timestamp": "timestamp",
                    "value": "amount",
                }
            )
        )
        return data

    @staticmethod
    def group_min_time(data: pd.DataFrame) -> pd.DataFrame:
        """
        根据 'from' 和 'to' 字段对数据进行分组，并计算每组的交易数量（transaction_id）、总金额（value）和最早交易时间（block_timestamp）。

        Args:
            data (pd.DataFrame): 输入的包含交易地址数据的 DataFrame。

        Returns:
            pd.DataFrame: 处理后的数据，其中包含 'src'、'dst'、'count'、'timestamp' 和 'amount' 这五个字段。
        """
        data = (
            data.pivot_table(
                index=["from", "to"],
                aggfunc={
                    "transaction_id": "count",
                    "value": "sum",
                    "block_timestamp": "min",
                    "time": "min",
                },
            )
            .reset_index()
            .rename(
                columns={
                    "from": "src",
                    "to": "dst",
                    "transaction_id": "count",
                    "block_timestamp": "timestamp",
                    "value": "amount",
                }
            )
        )
        return data

    async def get_group_trade(
        self,
        address,
        is_trx,
        start_timestamp,
        end_timestamp,
        only_form=False,
        only_to=False,
    ) -> pd.DataFrame:
        """聚合数据,统计每个地址与当前地址的交易次数和交易金额

        Args:
            address (str): 需要查询的交易地址。
            is_trx (bool): 指定是否为 TRX 链的交易记录，True 为 TRX 链，False 为其他链。
            only_form (bool, optional): 只查询转出. Defaults to False.
            only_to (bool, optional): 只查询转入. Defaults to False.

        Returns:
            dataframe: 未聚合的usdt交易记录
        """

        if not (only_form is False and only_to is False):
            data = await self.get_token_trade(
                address=address,
                is_trx=is_trx,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                only_form=only_form,
                only_to=only_to,
            )
            if data.empty:
                return data
            data["value"] = data["value"].astype(float)
            if only_form is True and only_to is False:
                data = self.group_min_time(data)
            elif only_to is True and only_form is False:
                data = self.group_max_time(data)
            return data
        # 如果同时需要查询转入和转出
        from_data = await self.get_token_trade(
            address=address,
            is_trx=is_trx,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            only_form=True,
            only_to=False,
        )
        from_data = self.group_min_time(from_data)

        to_data = await self.get_token_trade(
            address=address,
            is_trx=is_trx,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            only_form=False,
            only_to=True,
        )
        to_data = self.group_max_time(to_data)
        data = pd.concat([from_data, to_data], ignore_index=True)
        return data

    async def get_usdt_trade(
        self, address, start_timestamp, end_timestamp, only_form=None, only_to=None
    ):
        return await self.get_token_trade(
            address,
            False,
            start_timestamp,
            end_timestamp,
            "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            only_form,
            only_to,
        )

    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def __del__(self):
        asyncio.run_coroutine_threadsafe(self.close_session(), asyncio.get_event_loop())


class ParallelTrxAddress:
    def __init__(
        self,
        api_keys=None,
        proxy=None,
        num_processes=None,
        segments_num=None,
        platform=None,
    ):
        self.only_form = None
        self.is_trx = None
        self.token_id = None
        self.address = None
        self.only_to = None
        self.num_processes = num_processes if num_processes else math.ceil(os.cpu_count())
        self.proxy = proxy
        self.segments_num = segments_num
        self.api_keys = json.loads(os.environ["TRON_API_KEYS"]) if api_keys is None else api_keys
        self.pool = Pool(processes=self.num_processes)
        with open("platform.json", "r") as f:
            self.platform = json.loads(f.read()) if platform is None else platform
        self.trx_address = TrxAddress(
            api_keys=self.api_keys, platform=self.platform, proxy=self.proxy
        )
        self.rate_limiters = {
            platform_name
            + "---"
            + key: TokenProducer(
                platform["key_rate"] - 1,
                1,
                platform_name + "---" + key,
            )
            for platform_name, platform in self.platform.items()
            for key in self.api_keys[platform_name]
        }
        # 启动令牌生产任务
        self.token_producer_tasks = {
            platform_key: asyncio.create_task(self.rate_limiters[platform_key].produce())
            for platform_key, _ in self.rate_limiters.items()
        }

        self.trx_address.rate_limiters = self.rate_limiters

    async def get_transaction_counts(self, address, is_trx, token_id=None):
        if is_trx:
            return await self.trx_address.get_trx_transfer_quantity(address)
        return await self.trx_address.get_trc20_transfer_quantity(address, token_id)

    async def fetch_trans_time(self, address, token_id, is_trx):
        start_timestamp, end_timestamp = await self.trx_address.get_start_and_end_time(
            address, token_id, is_trx
        )
        return start_timestamp, end_timestamp

    @staticmethod
    def get_seg_num(trans_count):
        if trans_count < 800:
            return 1, 2
        if trans_count < 4000:
            return 4, 4
        if trans_count < 8000:
            return 4, 8
        return 10, 8

    async def parallel_fetch_groupp_data_for_period(
        self, address, start_timestamp, end_timestamp, query_type, num_processes
    ):
        duration = math.ceil((end_timestamp - start_timestamp) // num_processes)
        periods = [
            (address, start, start + duration, query_type)
            for start in range(start_timestamp, end_timestamp, duration)
        ]
        results = await self.pool.starmap(
            fetch_groupp_data_for_period,
            [
                (
                    address,
                    start,
                    start + duration,
                    query_type,
                    self.api_keys,
                    self.platform,
                    self.proxy,
                )
                for address, start, end, query_type in periods
            ],
        )

        def merge_lists(list1, list2):
            list1.extend(list2)
            return list1

        results = reduce(merge_lists, results)
        return results

    async def parallel_fetch_transfer_data(
        self, address, token_id, is_trx, only_form=False, only_to=False
    ):
        self.address = address
        self.token_id = token_id
        self.is_trx = is_trx
        self.only_form = only_form
        self.only_to = only_to
        start_timestamp, end_timestamp = await self.fetch_trans_time(
            address, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", False
        )
        in_count, out_count = await self.trx_address.get_transfer_count(
            address, False, only_form, only_to, token_id
        )
        if self.num_processes is None or self.segments_num is None:
            self.num_processes, self.segments_num = self.get_seg_num(in_count + out_count)
        daily_group_data = await self.parallel_fetch_groupp_data_for_period(
            address, start_timestamp, end_timestamp, "Transfer", self.num_processes // 4
        )

        trans_count = sum(day["transfer_count"] for day in daily_group_data)
        time_periods: list[TimePeriod] = TimePeriod(daily_group_data).get_cumsun_slide(
            "transfer_count", math.ceil(trans_count // self.num_processes)
        )
        intervals = [(period.start_time, period.end_time) for period in time_periods]
        intervals[0] = (
            min(start_timestamp, intervals[0][0]) - 86400000,
            intervals[0][1],
        )
        intervals[-1] = (
            intervals[-1][0],
            max(end_timestamp, intervals[-1][1]) + 86400000,
        )

        results = []
        for result in await self.pool.starmap(
            fetch_trans_data_for_period,
            [
                (
                    address,
                    start,
                    end,
                    token_id,
                    is_trx,
                    only_form,
                    only_to,
                    self.api_keys,
                    self.platform,
                    self.proxy,
                    self.segments_num,
                )
                for start, end in intervals
            ],
        ):
            results.append(result)

        combined_data = pd.concat([result[0] for result in results], ignore_index=True)

        return combined_data, sum(result[1] for result in results)

    async def close(self):
        if asyncio.current_task() is None:
            print("No active event loop, close_session will not be executed.")
            return
        await self.trx_address.close_session()  # 确保关闭 session
        # 关闭令牌生产任务
        for task in self.token_producer_tasks.values():
            while task.done() is False:
                await asyncio.sleep(0.1)
                task.cancel()
        await asyncio.gather(*self.token_producer_tasks.values(), return_exceptions=True)
        asyncio.run_coroutine_threadsafe(self.close(), asyncio.get_event_loop())
        self.pool.terminate()



if __name__ == "__main__":
    start_time = time.time()
