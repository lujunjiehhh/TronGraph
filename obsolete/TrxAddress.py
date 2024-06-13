import time

import base58
import json
import os
import pandas as pd
import requests
from TagCreeper import TagCreeper
from multiprocessing import Pool


class TrxAddress:
    def __init__(self, address: str):
        self.address = address
        self.proxies = {
            "http": "http://127.0.0.1:7890",
            "https": "http://127.0.0.1:7890",
        }
        self.trx_chain = []
        self.usdt_income = None
        self.usdt_outcome = None
        self.balance = None

    def is_contract_address(self):
        url = f"https://apilist.tronscanapi.com/api/contract?contract={self.address}"
        headers = {"TRON-PRO-API-KEY": "14a1b982-4e20-427e-83dc-82b972ff50d2"}
        json_data = requests.get(url=url, headers=headers).json()
        result = json_data["data"][0]["address"]
        if result != "":
            return True
        else:
            return False

    def get_start_and_end_time(self, is_trx):
        """获取USDT或者trx的首笔后最后一笔交易时间"""
        start_time = 0
        end_time = int(time.time()) * 1000
        if is_trx:
            url = f"https://www.oklink.com/api/v5/explorer/address/address-summary?chainShortName=tron&address={self.address}"
            headers = {"Ok-Access-Key": "c9005a90-be70-4dad-b785-7bded2f53878"}
            end_time = time.time()
            count = 0
            while count < 10:
                try:
                    json_data = requests.get(
                        url, headers=headers, proxies=self.proxies, timeout=10
                    ).json()
                    start_time = int(json_data["data"][0]["firstTransactionTime"])
                    end_time = int(json_data["data"][0]["lastTransactionTime"])
                    break
                except:
                    count += 1
                    continue
        else:
            headers = {
                "accept": "application/json",
                "TRON-PRO-API-KEY": "d0a2bb44-7e2a-4a54-913b-e1b41fc3db38",
            }
            count = 0
            while count < 10:
                try:
                    url = f"https://api.trongrid.io/v1/accounts/{self.address}/transactions/trc20?limit=1&order_by=block_timestamp%2Casc&min_timestamp=0&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
                    response = requests.get(
                        url, headers=headers, proxies=self.proxies, timeout=10
                    )
                    start_time = int(
                        json.loads(response.text)["data"][0]["block_timestamp"]
                    )

                    url = f"https://api.trongrid.io/v1/accounts/TZ5bRmFf66hQLKgLEUDSwyqJoxhp5apEji/transactions/trc20?limit=1&order_by=block_timestamp%2Cdesc&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
                    response = requests.get(
                        url, headers=headers, proxies=self.proxies, timeout=10
                    )
                    end_time = int(
                        json.loads(response.text)["data"][0]["block_timestamp"]
                    )
                    break
                except:
                    count += 1
                    continue

        return start_time, end_time

    def get_trans_data(self, is_trx, start_time, end_time, only_form, only_to):
        """用于多进程查询函数

        Args:
            only_form (bool, optional): 只查询转出. Defaults to False.
            only_to (bool, optional): 只查询转入. Defaults to False.

        Returns:
            _type_: _description_
        """
        # 时间戳json，根据json中每一个地址对应的时间戳拼凑url进行查询
        trans_data = pd.DataFrame()
        timestamp = start_time
        last_timestamp = start_time
        address = self.address
        next_url = ""
        count1 = 0
        last_len = 0
        while True:
            session = requests.session()
            count1 += 1
            # print(count)
            headers = {
                "accept": "application/json",
                "TRON-PRO-API-KEY": "d0a2bb44-7e2a-4a54-913b-e1b41fc3db38",
            }
            if is_trx is False:
                if only_form is True:
                    url = f"https://api.trongrid.io/v1/accounts/{address}/transactions/trc20?limit=200&order_by=block_timestamp%2Casc&min_timestamp={last_timestamp}&max_timestamp={end_time}&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t&only_from=true"
                elif only_to is True:
                    url = f"https://api.trongrid.io/v1/accounts/{address}/transactions/trc20?limit=200&order_by=block_timestamp%2Casc&min_timestamp={last_timestamp}&max_timestamp={end_time}&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t&only_to=true"
                elif only_form is False and only_to is False:
                    url = f"https://api.trongrid.io/v1/accounts/{address}/transactions/trc20?limit=200&order_by=block_timestamp%2Casc&min_timestamp={last_timestamp}&max_timestamp={end_time}&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
                else:
                    print("参数错误")
                    break
            else:
                if only_form is True:
                    url = f"https://api.trongrid.io/v1/accounts/{address}/transactions?limit=200&order_by=block_timestamp%2Casc&min_timestamp={last_timestamp}&max_timestamp={end_time}&only_from=true"
                elif only_to is True:
                    url = f"https://api.trongrid.io/v1/accounts/{address}/transactions?limit=200&order_by=block_timestamp%2Casc&min_timestamp={last_timestamp}&max_timestamp={end_time}&only_to=true"
                elif only_form is False and only_to is False:
                    url = f"https://api.trongrid.io/v1/accounts/{address}/transactions?limit=200&order_by=block_timestamp%2Casc&min_timestamp={last_timestamp}&max_timestamp={end_time}"
                else:
                    print("参数错误")
                    break
            count2 = 0
            response = None
            while count2 < 20:
                try:
                    response = session.get(
                        url, headers=headers, proxies=self.proxies, timeout=10
                    )
                    code = response.status_code
                    if code == 200:
                        break
                except Exception as e:
                    time.sleep(0.5)
                    count2 += 1
                    continue
            if response is None:
                print("数据获取失败")
                break
            # 当数据拉取完毕之时，我们判断，获取的数据为1条的时候就不拉取数据，以免一直拉取重复数据
            # print(len(json.loads(response.text)["data"]))
            if (
                len(json.loads(response.text)["data"]) == last_len
                and len(json.loads(response.text)["data"]) < 200
            ) or len(json.loads(response.text)["data"]) == 0:
                break
            # if len(json.loads(response.text)["data"]) <= 1:
            #     break
            last_len = len(json.loads(response.text)["data"])
            last_timestamp = json.loads(response.text)["data"][-1]["block_timestamp"]
            # 判断第一个时间戳是否与最后一个时间戳相等，如果相等，那么就使用next_url来获取数据
            if timestamp == last_timestamp:
                if next_url == "":
                    continue
                print(
                    str(os.getpid())
                    + str(timestamp)
                    + ":"
                    + str(json.loads(response.text)["data"][-1]["block_timestamp"])
                )
                url = next_url
                response = requests.get(
                    url, headers=headers, proxies=self.proxies, timeout=10
                )
                last_timestamp = json.loads(response.text)["data"][-1][
                    "block_timestamp"
                ]
            last_timestamp = json.loads(response.text)["data"][-1]["block_timestamp"]

            if is_trx is False:
                trans_data = trans_data._append(
                    json.loads(response.text)["data"]
                )  # type:ignore
            elif is_trx:
                for i in range(len(json.loads(response.text)["data"])):
                    try:
                        data = json.loads(response.text)["data"][i]["raw_data"][
                            "contract"
                        ][0]
                    except:
                        print("内部交易 跳过")
                        continue

                    if data["type"] == "TransferContract":
                        txID = json.loads(response.text)["data"][i]["txID"]

                        from_address: str = data["parameter"]["value"]["owner_address"]
                        from_address = base58.b58encode_check(
                            bytes.fromhex(from_address)
                        ).decode("utf-8")

                        to_address: str = data["parameter"]["value"]["to_address"]
                        to_address = base58.b58encode_check(
                            bytes.fromhex(to_address)
                        ).decode("utf-8")
                        timestamp = json.loads(response.text)["data"][i][
                            "block_timestamp"
                        ]
                        value = data["parameter"]["value"]["amount"]

                        trans_data = trans_data._append(
                            [
                                {
                                    "block_timestamp": timestamp,
                                    "from": from_address,
                                    "to": to_address,
                                    "value": value,
                                    "transaction_id": txID,
                                }
                            ]
                        )  # type:ignore
                        # print(txID, from_address, to_address, value, timestamp)
                    else:
                        continue

            try:
                finger_next = json.loads(response.text)["meta"]["links"]["next"]
            except Exception as e:
                finger_next = ""
            next_url = finger_next

        trans_data.drop_duplicates(
            subset=["transaction_id"], keep="first", inplace=True
        )
        if trans_data.empty:
            return trans_data
        else:
            trans_data["value"] = trans_data["value"].apply(lambda x: int(x) / 1e6)
            # timestamp转换为时间
            trans_data["time"] = trans_data["block_timestamp"].apply(
                lambda x: pd.Timestamp(x / 1000, unit="s", tz="Asia/Shanghai")
            )
            if is_trx is False:
                trans_data.drop(["token_info"], axis=1, inplace=True)
            return trans_data

    def get_first_trx_from(self):
        """获得第一笔交易记录

        Returns:
            address，tx_id: 交易地址和交易hash
        """
        url = f"https://api.trongrid.io/v1/accounts/{self.address}/transactions/?limit=1&order_by=block_timestamp%2Casc&min_timestamp=0"
        headers = {
            "accept": "application/json",
            "TRON-PRO-API-KEY": "d0a2bb44-7e2a-4a54-913b-e1b41fc3db38",
        }
        address = ""
        tx_id = ""
        count = 0
        while count < 10:
            try:
                response = requests.get(
                    url, headers=headers, proxies=self.proxies, timeout=10
                )
                hex_address: str = json.loads(response.text)["data"][0]["raw_data"][
                    "contract"
                ][0]["parameter"]["value"]["owner_address"]
                address = base58.b58encode_check(bytes.fromhex(hex_address)).decode(
                    "utf-8"
                )
                tx_id = json.loads(response.text)["data"][0]["txID"]
                break
            except:
                time.sleep(0.5)
                count += 1
                continue
        return address, tx_id

    def get_first_trx_chain(self, db, is_not_filter=False, is_not_stop=False):
        """获得第一笔trx的激活链，直到遇见第一个交易所地址"""
        collection = db["trx_gas"]

        if self.address in collection.distinct("_id"):
            document = collection.find_one({"_id": self.address})
            chain = pd.DataFrame(document.get("chain", []))
            self.trx_chain = document.get("trx_chain", [])
            return chain, self.trx_chain
        else:
            chain = []
            current_address = self.address
            tag = "未查询标签"
            if not is_not_filter:
                tag = TagCreeper().search(current_address)
            count = 0
            while count < 8 and (
                is_not_stop or tag == "无标签" or tag == "Gambling" or tag == "--"
            ):
                trx_from_address, tx_id = TrxAddress(
                    current_address
                ).get_first_trx_from()
                if not is_not_filter:
                    tag = TagCreeper().search(trx_from_address)
                chain.append(
                    {
                        "dst": current_address,
                        "src": trx_from_address,
                        "tx_id": tx_id,
                        "src_tag": tag,
                    }
                )
                if current_address == trx_from_address:
                    break
                current_address = trx_from_address
                count += 1

            chain_df = pd.DataFrame(chain)
            self.trx_chain = list(chain_df["dst"])

            data = {"_id": self.address, "trx_chain": self.trx_chain}
            collection.insert_one(data)

            return chain_df, self.trx_chain

    def get_token_trade(
        self,
        is_trx,
        start_or_end_time=0,
        only_form=False,
        only_to=False,
    ):
        """查询USDT转出转入记录

        Args:
            start_or_end_time (int, optional): 需要手动指定的结束时间或开始时间. Defaults to 0.
            only_form (bool, optional): 只查询转出. Defaults to False.
            only_to (bool, optional): 只查询转入. Defaults to False.

        Returns:
            dataframe: 未聚合的usdt交易记录
        """
        # 修改起始时间或者结束时间，使得资金流在时间上更加合理
        start_time, end_time = self.get_start_and_end_time(is_trx=False)

        if start_or_end_time != 0:
            if only_form is True:
                start_time = start_or_end_time
            elif only_to is True:
                end_time = start_or_end_time

        # 把时间段分成n个时间段,并加上only_form和only_to参数,满足get_trans_data的参数要求
        if not is_trx:
            if only_to:
                transfer_count = self.get_trc20_transfer_quantity()[0]
            elif only_form:
                transfer_count = self.get_trc20_transfer_quantity()[1]
            else:
                transfer_count = sum(self.get_trc20_transfer_quantity())
        else:
            transfer_count = self.get_trx_transfer_quantity()

        if transfer_count < 800:
            processing_count = 1
        elif transfer_count < 2400:
            processing_count = 2
        elif transfer_count < 4000:
            processing_count = 4
        else:
            processing_count = 8
        # 避免transfer_count过大
        if transfer_count > 800000:
            print("transfer_count过大，跳过")
            return pd.DataFrame()
        print("processing_count:", processing_count)
        print("transfer_count:", transfer_count)
        if processing_count > 1:
            interval = (end_time - start_time) // processing_count - 1
            time_intervals = [
                (
                    is_trx,
                    start_time + i * interval,
                    start_time + (i + 1) * interval,
                    only_form,
                    only_to,
                )
                for i in range(processing_count - 1)
            ]
            # 加上最后一个时间段
            time_intervals.append(
                (is_trx, time_intervals[-1][1], end_time, only_form, only_to)
            )
            # print(time_intervals)
            # 开n个进程，每个进程处理一个时间段
            # 用map函数把n个时间段分配给n个进程
            pool = Pool(processes=processing_count)
            dataframes = pool.map(self.get_trans_data_for_pickle, time_intervals)
            pool.close()
            pool.join()
            # 把15个dataframe合并成一个dataframe
            data = pd.concat(dataframes, ignore_index=True)
        else:
            data = self.get_trans_data(is_trx, start_time, end_time, only_form, only_to)
        # 去重
        data.drop_duplicates(subset=["transaction_id"], keep="first", inplace=True)
        # 打印交易次数
        return data

    def get_trans_data_for_pickle(self, args):
        """这里需要单独声明，防止无法pickle"""
        return self.get_trans_data(*args)

    def get_group_trade(
        self, is_trx, start_or_end_time=0, only_form=False, only_to=False
    ) -> pd.DataFrame:
        """聚合数据,统计每个地址与当前地址的交易次数和交易金额

        Args:
            start_or_end_time (int, optional): 需要手动指定的结束时间或开始时间. Defaults to 0.
            only_form (bool, optional): 只查询转出. Defaults to False.
            only_to (bool, optional): 只查询转入. Defaults to False.

        Returns:
            dataframe: 未聚合的usdt交易记录
        """

        def group_max_time(data: pd.DataFrame) -> pd.DataFrame:
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

        def group_min_time(data: pd.DataFrame) -> pd.DataFrame:
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

        if not (only_form is False and only_to is False):
            data = self.get_token_trade(
                is_trx=is_trx,
                start_or_end_time=start_or_end_time,
                only_form=only_form,
                only_to=only_to,
            )
            if data.empty:
                return data
            else:
                data["value"] = data["value"].astype(float)
                if only_form is True and only_to is False:
                    data = group_min_time(data)
                elif only_to is True and only_form is False:
                    data = group_max_time(data)
                return data
        # 如果同时需要查询转入和转出
        else:
            from_data = self.get_token_trade(
                is_trx=is_trx,
                start_or_end_time=start_or_end_time,
                only_form=True,
                only_to=False,
            )
            from_data = group_min_time(from_data)

            to_data = self.get_token_trade(
                is_trx=is_trx,
                start_or_end_time=start_or_end_time,
                only_form=False,
                only_to=True,
            )
            to_data = group_max_time(to_data)
            data = pd.concat([from_data, to_data], ignore_index=True)
            return data

    def get_trx_transfer_quantity(self):
        url = f"https://www.oklink.com/api/v5/explorer/address/address-summary?chainShortName=tron&address={self.address}"
        headers = {"Ok-Access-Key": "c9005a90-be70-4dad-b785-7bded2f53878"}
        json_data = requests.get(
            url, headers=headers, proxies=self.proxies, timeout=10
        ).json()
        return int(json_data["data"][0]["transactionCount"])

    def get_trc20_transfer_quantity(
        self, token_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    ):
        url = f"https://apilist.tronscanapi.com/api/deep/account/holderToken/basicInfo/trc20/transfer?accountAddress={self.address}&tokenAddress={token_address}"
        headers = {
            "accept": "application/json",
            "TRON-PRO-API-KEY": "14a1b982-4e20-427e-83dc-82b972ff50d2",
        }
        response = requests.get(
            url, headers=headers, proxies=self.proxies, timeout=10
        ).json()
        return response["transferIn"], response["transferOut"]

    def get_trc20_token_quantity(
        self, token_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    ):
        """基于余额计算usdt的交易总量"""
        in_count, out_count = self.get_trc20_transfer_quantity()
        url = f"https://www.oklink.com/api/v5/explorer/address/address-balance-fills?chainShortName=tron&address={self.address}&protocolType=token_20&tokenContractAddress={token_address}"
        headers = {"Ok-Access-Key": "c9005a90-be70-4dad-b785-7bded2f53878"}
        try:
            response = requests.get(url=url, headers=headers, timeout=10).json()
            balance = float(response["data"][0]["tokenList"][0]["holdingAmount"])
        except:
            balance = 0
        if in_count > out_count:
            # 如果
            if self.get_group_trade(is_trx=False, only_form=True).empty is False:
                out_amount = float(
                    self.get_group_trade(is_trx=False, only_form=True)["amount"].sum()
                )
            else:
                out_amount = 0
            in_amount = balance + out_amount
        else:
            # 如果不为空
            if self.get_group_trade(is_trx=False, only_to=True).empty is False:
                in_amount = float(
                    self.get_group_trade(is_trx=False, only_to=True)["amount"].sum()
                )
            else:
                in_amount = 0
            out_amount = in_amount - balance
        self.usdt_income = in_amount
        self.usdt_outcome = out_amount
        self.balance = balance
        return in_amount, out_amount

    def get_first_trx_min_common_level(self, path1: list[int], path2: list[int]):
        """获得两个地址的最小公共祖先的层数"""
        common_nodes = set(path1) & set(path2)
        if common_nodes:
            common_node = min(common_nodes, key=path1.index)
            # 下取整
            return int((path1.index(common_node) + path2.index(common_node) + 2) / 2)
        else:
            return 999


if __name__ == "__main__":
    trx_address = TrxAddress(address="TCuUrTZ4rNKuzbA8DkXZQEgzXKTdZBANFQ")
    # trx_address.get_group_usdt_trade(
    #     only_form=True).to_csv('test.csv', index=False)
    # print(trx_address.get_trc20_token_quantity())

    # trx_address.get_first_trx_chain(is_not_filter=False, is_not_stop=True)[0].to_csv(
    #     "test_trx_chain.csv", index=False
    # )

    # # print(trx_address.get_first_trx_from()[0])aaaaaaaaaaaaaaaaaaaaaaaaaaaa
    # def handle_address(address):
    #     return oc.get_address_tag(address)[0]

    # #读取Excel
    # df = pd.read_excel('/home/lujunjie/Work/虞光照转出.xlsx')
    # # 添加tag列
    # df['tag'] = df['address'].apply(lambda x: handle_address(x))
    # df.to_excel('/home/lujunjie/Work/虞光照转出2.xlsx', index=False)

    # print(trx_address.get_trc20_token_quantity())

    trx_address.get_group_trade(is_trx=True).to_csv("test.csv", index=False)
