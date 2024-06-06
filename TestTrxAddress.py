import time

import base58
import json
import pandas as pd
import requests
from TagCreeper import TagCreeper
import numpy as np


def get_first_trx_min_common_level_numpy(path1, path2):
    """获得两个地址的最小公共祖先的层数"""
    _path1 = np.array(path1)
    _path2 = np.array(path2)
    _path1 = _path1[_path1 != np.array(None)]
    _path2 = _path2[_path2 != np.array(None)]
    try:
        if _path1 is not None and _path2 is not None:
            common_nodes, index1, index2 = np.intersect1d(_path1, _path2, assume_unique=True, return_indices=True)
            if index1.any():
                # 下取整
                return int((index1.min() + 1 + index2.min() + 1) / 2)
            else:
                return 999
        else:
            return 0
    except:
        print(_path1)
        print(_path2)


class TrxAddress:
    data = pd.read_csv("新币_edges.csv")

    # 读取json文件,转换为字典，查看字典中是否存在地址的gas激活记录，存在就直接返回
    def __init__(self, address: str):
        self.address = address  # type: str
        self.proxies = {
            "http": "http://127.0.0.1:7890",
            "https": "http://127.0.0.1:7890",
        }
        self.trx_chain = []
        self.usdt_income = None
        self.usdt_outcome = None
        self.balance = None

    def get_group_trade(
            self, is_trx, start_or_end_time=0, only_form=False, only_to=False
    ) -> pd.DataFrame:
        """用于测试的函数，输出交易记录"""
        # 读入用于测试的交易记录
        return self.data[self.data["src"] == self.address].copy()

    def get_start_and_end_time(self, is_trx):
        """获取USDT或者trx的首笔后最后一笔交易时间"""
        if is_trx:
            url = f"https://www.oklink.com/api/v5/explorer/address/address-summary?chainShortName=tron&address={self.address}"
            headers = {"Ok-Access-Key": "c9005a90-be70-4dad-b785-7bded2f53878"}
            json_data = requests.get(
                url, headers=headers, proxies=self.proxies, timeout=10
            ).json()
            start_time = int(json_data["data"][0]["firstTransactionTime"])
            end_time = int(json_data["data"][0]["lastTransactionTime"])

        else:
            headers = {
                "accept": "application/json",
                "TRON-PRO-API-KEY": "d0a2bb44-7e2a-4a54-913b-e1b41fc3db38",
            }

            url = f"https://api.trongrid.io/v1/accounts/{self.address}/transactions/trc20?limit=1&order_by=block_timestamp%2Casc&min_timestamp=0&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
            response = requests.get(
                url, headers=headers, proxies=self.proxies, timeout=10
            )
            start_time = int(json.loads(response.text)["data"][0]["block_timestamp"])

            url = f"https://api.trongrid.io/v1/accounts/TZ5bRmFf66hQLKgLEUDSwyqJoxhp5apEji/transactions/trc20?limit=1&order_by=block_timestamp%2Cdesc&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
            response = requests.get(
                url, headers=headers, proxies=self.proxies, timeout=10
            )
            end_time = int(json.loads(response.text)["data"][0]["block_timestamp"])

        return start_time, end_time

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

    # def get_first_trx_chain(
    #     self, lock, sheel_trx_chain_dict, is_not_filter=False, is_not_stop=False
    # ):
    #     """获得第一笔trx的激活链，直到遇见第一个交易所地址"""
    #     if sheel_trx_chain_dict.get(self.address) is not None:
    #         # with lock:
    #         chain = pd.DataFrame()
    #         self.trx_chain = sheel_trx_chain_dict.get(self.address)
    #         return chain, self.trx_chain
    #     else:
    #         chain = []
    #         current_address = self.address
    #         tag = "未查询标签"
    #         if is_not_filter is False:
    #             tag = TagCreeper().search(current_address)
    #         count = 0
    #         while count < 8 and (
    #             is_not_stop or tag == "无标签" or tag == "Gambling" or tag == "--"
    #         ):
    #             trx_from_address, tx_id = TrxAddress(
    #                 current_address
    #             ).get_first_trx_from()
    #             if is_not_filter is False:
    #                 tag = TagCreeper().search(trx_from_address)
    #             chain.append(
    #                 {
    #                     "dst": current_address,
    #                     "src": trx_from_address,
    #                     "tx_id": tx_id,
    #                     "src_tag": tag,
    #                 }
    #             )
    #             if current_address == trx_from_address:
    #                 break
    #             current_address = trx_from_address
    #             count += 1
    #         chain_df = pd.DataFrame(chain)
    #         self.trx_chain = list(chain_df["dst"])
    #         with lock:
    #             sheel_trx_chain_dict[self.address] = self.trx_chain
    #             # trx_chain_dict = dict(sheel_trx_chain_dict)
    #             trx_chain_dict = sheel_trx_chain_dict
    #             with open("trx_gas.json", "w") as f:
    #                 json.dump(trx_chain_dict, f)
    #         return chain_df, self.trx_chain

    def get_first_trx_chain(self, is_not_filter=False, is_not_stop=False):
        """获得第一笔trx的激活链，直到遇见第一个交易所地址"""
        chain = []
        current_address = self.address
        tag = "未查询标签"
        if not is_not_filter:
            tag = TagCreeper().search(current_address)
        count = 0
        while count < 8 and (
                is_not_stop or tag == "无标签" or tag == "Gambling" or tag == "--"
        ):
            trx_from_address, tx_id = TrxAddress(current_address).get_first_trx_from()
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
        return chain_df, self.trx_chain

    def get_first_trx_chain_for_tron_graph(
            self, db, is_not_filter=False, is_not_stop=False
    ):
        """获得第一笔trx的激活链，直到遇见第一个交易所地址"""
        collection = db["trx_gas"]

        if self.address in collection.distinct("address"):
            # print(self.address, "已经存在")
            document = collection.find_one({"address": self.address})

            try:
                self.trx_chain = document.get("chain")
            except Exception as e:
                print(e)

            return self.trx_chain
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
                chain.append(current_address)
                if current_address == trx_from_address:
                    break
                current_address = trx_from_address
                count += 1

            self.trx_chain = list(chain)
            data = {"address": self.address, "chain": self.trx_chain}
            try:
                collection.insert_one(data)
            except Exception as e:
                print(e)
            return self.trx_chain

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


if __name__ == "__main__":
    trx_address = TrxAddress(address="TP5Ct1fCmhRConWimVpx4U9MwiMuExi9m2")

    import pymongo

    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = client["trx_gas"]

    print(trx_address.get_first_trx_chain_for_tron_graph(db=db, is_not_filter=True, is_not_stop=True))
