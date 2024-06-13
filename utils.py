import asyncio
import bisect
import math
import time
from collections import Counter
from collections.abc import Mapping
from datetime import datetime

import aioredis


class RateLimiteException(Exception):
    def __init__(self, exception):
        super().__init__(exception)


class RateLimiter:
    def __init__(self, rate, per, key, host="localhost", port=6379, db=0):
        self.rate = rate
        self.per = per
        self.key = key
        self.redis_client = aioredis.from_url(f"redis://{host}:{port}/{db}")

    async def acquire(self):
        while True:
            now = time.time()
            async with self.redis_client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(self.key)
                    tokens = float(await pipe.get(self.key) or self.rate)
                    elapsed = now - float(await pipe.get(f"{self.key}_last") or now)
                    tokens += elapsed * (self.rate / self.per)
                    tokens = min(tokens, self.rate)
                    if tokens >= 1:
                        tokens -= 1
                        tokens = int(tokens)
                        pipe.multi()
                        await pipe.set(self.key, tokens)
                        await pipe.set(f"{self.key}_last", now)
                        await pipe.execute()
                        break
                except aioredis.WatchError:
                    continue
                finally:
                    await pipe.reset()

    async def get_tokens(self):
        while True:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(self.key)
                    tokens = float(await pipe.get(self.key) or self.rate)
                    break
                except aioredis.WatchError:
                    continue
                finally:
                    await pipe.reset()
        return tokens

    async def get_all_usable_apikey(self, platform):
        cursor = "0"
        available_keys = []
        while cursor != 0:
            cursor, keys = await self.redis_client.scan(
                cursor=cursor, match=f"{platform}*_stop"
            )
            for key in keys:
                stop_key = key.decode("utf-8")
                platform_key = stop_key.replace("_stop", "").replace(
                    f"{platform}---", ""
                )
                if await self.redis_client.get(stop_key) == b"0":
                    available_keys.append(platform_key)
        return list(set(available_keys))

    async def notify_stop_production(self, stop_seconds):
        stop_key = f"{self.key}_stop"
        await self.redis_client.set(stop_key, b"1", ex=stop_seconds)


class TokenProducer(RateLimiter):
    def __init__(self, rate, per, key, host="localhost", port=6379, db=0):
        super().__init__(rate, per, key)
        self.redis_client = aioredis.from_url(f"redis://{host}:{port}/{db}")
        self.pause_event = asyncio.Event()
        self.pause_event.set()  # Initially not paused

    async def produce(self):
        stop_key = f"{self.key}_stop"

        await self.redis_client.set(stop_key, b"0")
        try:
            while True:
                stop_production = await self.redis_client.get(stop_key)
                if stop_production == b"1":
                    await asyncio.sleep(1)  # 每秒检查一次是否可以恢复生产
                    continue

                now = time.time()
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    try:
                        await pipe.watch(self.key)
                        tokens = float(await pipe.get(self.key) or 0)
                        elapsed = now - float(await pipe.get(f"{self.key}_last") or now)
                        new_tokens = elapsed * (self.rate / self.per)
                        tokens += min(new_tokens, self.rate - tokens)
                        pipe.multi()
                        await pipe.set(self.key, min(tokens, 3))  # 确保令牌数不超过5
                        await pipe.set(f"{self.key}_last", now)
                        await pipe.execute()
                        await asyncio.sleep(
                            (self.per / self.rate) * 3
                        )  # 按照速率进行 sleep
                        continue
                    except aioredis.WatchError:
                        continue
                    finally:
                        await pipe.reset()

        except asyncio.CancelledError:
            await self.redis_client.close()

    async def pause(self, seconds):
        self.pause_event.clear()  # Pause production
        await asyncio.sleep(seconds)
        self.pause_event.set()  # Resume production


class DailyData(Mapping):

    def __len__(self):
        return self.daily_data.__len__()

    def __iter__(self):
        return iter(self.daily_data)

    def __init__(self, data):
        if isinstance(data, dict):
            self.day = self._to_timestamp(data["day"])
            del data["day"]
            self.daily_data = data

    def __hash__(self):
        return self.day

    def __getitem__(self, key):
        # 自定义取值行为
        return self.daily_data[key]

    def __setitem__(self, key, value):
        # 自定义赋值行为
        self.daily_data[key] = value

    @staticmethod
    def _to_timestamp(time_str):
        # 将时间字符串转换为datetime对象
        if not isinstance(time_str, int):
            date_object = datetime.strptime(time_str, "%Y-%m-%d")
            # 使用 timestamp 方法将datetime对象转换为时间戳
            timestamp = int(date_object.timestamp() * 1000)
            return timestamp
        return time_str

    def __delitem__(self, key):
        del self.daily_data[key]

    def remove_key(self, key):
        if key in self.daily_data.keys():
            del self[key]
        return self


class TimePeriod:
    def __init__(self, data=None, value_sum=None):
        """
        :param value_sum
        :type value_sum: Counter
        :param data:
        :type data:
        """
        self.start_time = 0
        self.end_time = 0
        self.data = data
        self.value_sum = value_sum

        if data:
            if isinstance(self.data[0], dict):
                self.data = [DailyData(day) for day in data]
            self.start_time = self.data[0].day
            self.end_time = self.data[-1].day
            self.value_sum: Counter = self.get_value_sum(self.data)
        else:
            self.start_time = 0
            self.end_time = 0

    @staticmethod
    def get_value_sum(data) -> Counter:
        total = Counter()
        for value in data:
            total.update(Counter(value))
        return total

    def __getitem__(self, index):
        # 自定义取值行为
        return self.data[index]

    def __iter__(self):
        return iter(self.data)

    def __setitem__(self, key, value):
        self.data[key] = value

    def __add__(self, other):
        if not isinstance(other, TimePeriod):
            return NotImplemented
        # 部分重叠或完全重叠的情况
        merged_data = list(set(self.data) | set(other.data))
        return TimePeriod(merged_data)

    def __and__(self, other):
        if not isinstance(other, TimePeriod):
            return NotImplemented
        if self.end_time <= other.start_time or other.end_time <= self.start_time:
            return TimePeriod(data=[], value_sum=Counter())
        if not self.data or not other.data:
            return TimePeriod(data=[], value_sum=Counter())
        new_data = list(set(self.data) & set(other.data))
        return TimePeriod(new_data)

    def __len__(self):
        return self.data.__len__()

    @staticmethod
    def slide(data, a, c):
        # 找到第一个大于等于low的元素的索引
        start_index = bisect.bisect_left(
            data, DailyData({"day": a}), key=lambda x: x.day
        )
        end_index = bisect.bisect_right(
            data, DailyData({"day": c}), key=lambda x: x.day
        )
        data = data[start_index:end_index]

        return data

    def __sub__(self, other):
        if not isinstance(other, TimePeriod):
            return NotImplemented
        a, b = self.start_time, self.end_time
        c, d = other.start_time, other.end_time
        # 如果两个时间段完全不重叠
        if b <= c or d <= a:
            return self

        result = []
        # 部分重叠的情况
        self.data = list(set(self.data) - set(other.data))
        if a < c:
            result.append(self.slide(self.data, a, c))
        if d < b:
            result.append(self.slide(self.data, d, b))
        return TimePeriod(data=result, value_sum=self.get_value_sum(result))

    def __truediv__(self, divisor):
        duration = math.ceil((self.end_time - self.start_time) // divisor)
        periods = []
        for i in range(divisor):
            start_timestamp = self.start_time + i * duration
            end_timestamp = self.start_time + (i + 1) * duration
            data = self.slide(self.data, start_timestamp, end_timestamp)
            value_sum = self.get_value_sum(data)
            periods.append(TimePeriod(data, value_sum))
        return periods

    def get_cumsun_slide(self, key, nums):
        if self.data is None:
            return []
        self.data = sorted(self.data, key=lambda x: x.day)
        result = []
        if not self.data:
            return result  # 如果数据为空，直接返回空列表
        if len(self.data) == 1:
            return [self]
        current_period = TimePeriod(data=[self[0]])
        pre_timestamp = i = 0
        while i < len(self):
            data = self[i]
            if current_period.value_sum.get(key, 0) <= nums:
                i += 1
                current_period += TimePeriod(data=[data])
            else:
                # 不是第一段
                if pre_timestamp != 0:
                    current_period.start_time = pre_timestamp
                pre_timestamp = current_period.end_time
                result.append(current_period)
                current_period = TimePeriod(data=[data])

        if (
            current_period.data and current_period.end_time != result[-1].end_time
        ):  # 确保 current_period 不是空的, 并且没有重复添加
            if pre_timestamp != 0:
                current_period.start_time = pre_timestamp
            result.append(current_period)
        return result
