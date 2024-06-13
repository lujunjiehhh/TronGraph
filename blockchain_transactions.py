import asyncio
import math

import motor.motor_asyncio
import pandas as pd
from pandas import Timestamp
from pymongo import ReturnDocument
from tqdm import tqdm
from pymongo import UpdateOne
from neo4j_wrapper import Neo4jWrapper, DataConverter
from trx_address import ParallelTrxAddress
from collections import defaultdict

class BlockchainTransactions:
    def __init__(
        self,
        mongo_url="mongodb://localhost:27017/",
        neo4j_url="neo4j://localhost:7687",
        neo4j_user="neo4j",
        neo4j_passwd="N@eo4j852853",
        proxy="http://127.0.0.1:1080", 
    ):
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)
        self.db = self.mongo_client["tron"]
        self.transactions = self.db["transactions"]

        self.counter_collection_address = self.db["address_counters"]
        self.counter_collection_pair = self.db["pair_counters"]
        self.neo4j = Neo4jWrapper(url=neo4j_url, password=neo4j_passwd, user=neo4j_user)
        self.address_mapping = self.db["address_mapping"]
        self.pair_mapping = self.db["pari_mapping"]
        self.trx_address = ParallelTrxAddress(proxy=proxy)

    async def check_addresses_exist(self, addresses):
        existing_addresses = self.address_mapping.find({"address": {"$in": addresses}})
        return {doc["address"] for doc in existing_addresses.to_list(1)}

    async def insert_addresses(self, addresses):
        address_docs = [{"address": address} for address in addresses]
        await self.address_mapping.insert_many(address_docs)

    async def batch_process_addresses(self, addresses):
        # 检查 MongoDB 中已存在的地址
        existing_addresses_mongo = await self.check_addresses_exist(addresses)
        new_addresses_mongo = [
            addr for addr in addresses if addr not in existing_addresses_mongo
        ]

        # 批量插入新的地址到 MongoDB
        if new_addresses_mongo:
            await self.insert_addresses(new_addresses_mongo)

        # 检查 Neo4j 中已存在的地址
        existing_addresses_neo4j = await self.neo4j.check_nodes_exist(addresses)
        new_addresses_neo4j = [
            addr for addr in addresses if addr not in existing_addresses_neo4j
        ]

        # 批量插入新的地址到 Neo4j
        # if new_addresses_neo4j:
        #     await self.neo4j.insert_nodes(new_addresses_neo4j)

    async def init_counter(self):
        # 初始化计数器文档，如果不存在则创建
        if not await self.counter_collection_address.find_one({"_id": "address_id"}):
            await self.counter_collection_address.insert_one(
                {"_id": "address_id", "seq": 0}
            )
        if not await self.counter_collection_pair.find_one({"_id": "pair_id"}):
            await self.counter_collection_pair.insert_one({"_id": "pair_id", "seq": 0})

    async def get_next_address_id(self):
        # 使用find_one_and_update原子操作获取下一个编号
        counter_doc = await self.counter_collection_address.find_one_and_update(
            {"_id": "address_id"},
            {"$inc": {"seq": 1}},
            return_document=ReturnDocument.AFTER,
        )
        return counter_doc["seq"]

    async def get_next_pair_id(self):
        # 使用find_one_and_update原子操作获取下一个编号
        counter_doc = await self.counter_collection_pair.find_one_and_update(
            {"_id": "pair_id"},
            {"$inc": {"seq": 1}},
            return_document=ReturnDocument.AFTER,
        )
        return counter_doc["seq"]

    async def get_address_id(self, address):
        address_doc = await self.address_mapping.find_one({"address": address})
        if not address_doc:
            address_id = await self.get_next_address_id()
            await self.address_mapping.insert_one(
                {"_id": address_id, "address": address}
            )
        else:
            address_id = address_doc["_id"]
        return address_id

    async def get_id_by_address(self, address):
        # 根据地址查找对应的ID
        address_doc = await self.address_mapping.find_one({"address": address})
        return address_doc["_id"] if address_doc else None

    async def get_pair_id(self, sender, receiver):
        center, address, in_out = await self.get_direction(sender, receiver)
        center_id, address_id = await self.get_address_id(
            center
        ), await self.get_address_id(address)
        pair = f"{center_id}<->{address_id}"
        pair_doc = await self.pair_mapping.find_one({"pair": pair})
        if not pair_doc:
            pair_id = await self.get_next_pair_id()
            await self.pair_mapping.insert_one({"_id": pair_id, "pair": pair})
        else:
            pair_id = pair_doc["_id"]
        if in_out == "in":
            return pair_id, {
                "in_out": in_out,
                f"{center_id}": sender,
                f"{address_id}": receiver,
            }
        elif in_out == "out":
            return pair_id, {
                in_out: in_out,
                f"{center_id}": receiver,
                f"{address_id}": sender,
            }

    async def store_single_transaction(self, sender, receiver, transaction):
        center, address, in_out = await self.get_direction(sender, receiver)
        center_id, address_id = await self.get_address_id(
            center
        ), await self.get_address_id(address)
        if sender == center:
            transaction["sender"] = center_id
            transaction["receiver"] = address_id
        elif sender == address:
            transaction["sender"] = address_id
            transaction["receiver"] = center_id

        pair_id, _ = await self.get_pair_id(center, address)
        filter_criteria = {"pair_id": pair_id, "in_out": in_out}
        update_operation = {"$push": {"transactions": transaction}}
        await self.transactions.update_one(
            filter_criteria, update_operation, upsert=True
        )

    async def store_transactions_batch(self, transactions):
        # 将DataFrame转换为字典列表，以便批量插入
        transactions_dicts = transactions.to_dict("records")
              
        result = await self.transactions.insert_many(transactions_dicts)
        # 输出插入的文档的ID
        print(result.inserted_ids)

    @staticmethod
    async def get_direction(sender, receiver):
        center, address = sorted([sender, receiver])
        in_out = "in" if sender == center else "out"
        return center, address, in_out

    async def get_transactions(self, address1, address2, in_out=None):
        
        pair_id, addresses_id = await self.get_pair_id(address1, address2)
        df_in = df_out = pd.DataFrame()

        if in_out == "in" or in_out is None:
            df_in = await self._get_transactions(pair_id, "in")
        if in_out == "out" or in_out is None:
            df_out = await self._get_transactions(pair_id, "out")
        df = pd.concat([df_in, df_out], ignore_index=True)
        
        print(addresses_id)
        print(df.head(1)["sender"].values[0])
        print(addresses_id[df.head(1)["sender"].values[0]])
        df["sender"] = df["sender"].to  .map(addresses_id)
        df["receiver"] = df["receiver"].str.map(addresses_id)
        return df

    async def _get_transactions(self, pair_id, in_out):
        # 创建游标并等待 to_list 完成
        cursor = self.transactions.find({"pair_id": pair_id, "in_out": in_out})
        results = await cursor.to_list(1)
        df = pd.DataFrame()
        if results:
            transactions = []
            for result in results:
                transactions.extend(result.get("transactions", []))
            df = pd.DataFrame(transactions)
        return df

    async def get_all_usdt_trans_records(self, addresses):
        for address in addresses:
            result = await self.trx_address.parallel_fetch_transfer_data(
                address,
                "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
                False,
            )
            result[0].drop_duplicates(
                subset=["transaction_id"], keep="first", inplace=True
            )
            result[0]["token_address"] = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
            result[0]["symbol"] = "USDT"
            result[0]["time"] = pd.to_datetime(result[0]["time"]).dt.tz_convert("UTC")
            result[0]["local_time"] = result[0]["time"].dt.tz_convert("Asia/Shanghai")
            result[0]["is_confirmed"] = True
            result[0].rename(
                columns={
                    "transaction_id": "transaction_hash",
                    "time": "utc_time",
                    "from": "sender",
                    "to": "receiver",
                    "value": "amount",
                    "symbol": "symbol",
                    "token_address": "token_address",
                    "is_confirmed": "is_confirmed",
                },
                inplace=True,
            )
            result[0].to_csv(f"{address}_transactions1.csv", index=False)
            await self.store_dataframe(result[0])
            print(address, result[1], result[0].shape)
        return result[0]

    async def close(self):
        await self.trx_address.close()
        self.mongo_client.close()

    async def store_dataframe(self, df):
        center, address, _ = await self.get_direction(
            df.loc[0, "sender"], df.loc[0, "receiver"]
        )
        pre_out = (await self.neo4j.edges({"id": center}, {"id": address}))[0]
        pre_in = (await self.neo4j.edges({"id": address}, {"id": center}))[0]
        if pre_out and pre_out != {}:
            pre_out_data = pre_out["r"]
            df_out = df[df["sender"] == center]
            new_out_count = len(df_out)
            new_last_time = df_out.iloc[-1]["local_time"]
            new_first_time = df_out.iloc[0]["local_time"]
            pre_out_count = pre_out_data["usdt_out_count"]
            if (
                pre_out_count < new_out_count
                or new_last_time > pre_out_data["last_usdt_time"]
                or pre_out_data["first_usdt_time"] > new_first_time
            ):
                pre_df_out = await self.get_transactions(center, address, "out")
                await self._add_df(df_out, pre_df_out, pre_out_data)
        else:
            df_out = df[df["sender"] == center]
            pre_out_data = {
                "first_usdt_time": df_out["local_time"].min(),
                "last_usdt_time": df_out["local_time"].max(),
                "transcations_count": len(df_out),
                "usdt_amount": sum(df_out["amount"]),
            }
            await self._add_df(df_out, pd.DataFrame(), pre_out_data)

        if pre_in:
            pre_in_data = pre_in["r"]
            df_in = df[df["sender"] == address]
            new_in_count = len(df_in)
            new_last_time = df_in.iloc[-1]["local_time"]
            new_first_time = df_in.iloc[0]["local_time"]
            pre_in_count = pre_in_data["usdt_in_count"]
            if (
                pre_in_count < new_in_count
                or new_last_time > pre_in_data["last_usdt_time"]
                or pre_in_data["first_usdt_time"] > new_first_time
            ):
                pre_df_in = await self.get_transactions(center, address, "in")
                await self._add_df(df_in, pre_df_in, pre_in_data)
        else:
            df_in = df[df["sender"] == address]
            pre_in_data = {
                "first_usdt_time": df_in["local_time"].min(),
                "last_usdt_time": df_in["local_time"].max(),
                "transcations_count": len(df_in),
                "usdt_amount": sum(df_in["amount"]),
            }
            await self._add_df(df_in, pd.DataFrame(), pre_in_data)

    async def _add_df(self, df: pd.DataFrame, pre_df, pre_edge_data):
        """
        将新的交易数据帧(df)与之前的交易数据帧(pre_df)合并，并更新到Neo4j图数据库中。
        同时，根据新的交易数据更新发送者和接收者的统计信息。

        参数:
        df: pandas.DataFrame, 新的交易数据帧。
        pre_df: pandas.DataFrame, 已有的交易数据帧。
        pre_edge_data: dict, 已有的边的数据，用于更新。
        """
        if df.empty:
            return
        pre_start_address = df.head(1)["sender"].values[0]
        # 获取df中第一个交易的发送者和接收者在Neo4j中的节点对象。
        pre_start = (await self.neo4j.nodes([pre_start_address]))[0]
        if pre_start != {}:
            pre_start_data = pre_start
        else:
            pre_start_data = {
                "usdt_in_count": 0,
                "usdt_in_amount": 0,
                "usdt_out_count": 0,
                "usdt_out_amount": 0,
                "usdt_in_end_time": int(
                    Timestamp("1970-01-01 00:00:00", tz="UTC").timestamp()
                ),
                "usdt_in_start_time": int(
                    Timestamp("2099-01-01 00:00:00", tz="UTC").timestamp()
                ),
                "usdt_out_end_time": int(
                    Timestamp("1970-01-01 00:00:00", tz="UTC").timestamp()
                ),
                "usdt_out_start_time": int(
                    Timestamp("2099-01-01 00:00:00", tz="UTC").timestamp()
                ),
            }
        pre_end_address = df.head(1)["receiver"].values[0]
        pre_end = (await self.neo4j.nodes([pre_end_address]))[0]
        if pre_end != {}:
            pre_end_data = pre_end
        else:
            pre_end_data = {
                "usdt_in_count": 0,
                "usdt_in_amount": 0,
                "usdt_out_count": 0,
                "usdt_out_amount": 0,
                "usdt_in_end_time": int(
                    Timestamp("1970-01-01 00:00:00", tz="UTC").timestamp()
                ),
                "usdt_in_start_time": int(
                    Timestamp("2099-01-01 00:00:00", tz="UTC").timestamp()
                ),
                "usdt_out_end_time": int(
                    Timestamp("1970-01-01 00:00:00", tz="UTC").timestamp()
                ),
                "usdt_out_start_time": int(
                    Timestamp("2099-01-01 00:00:00", tz="UTC").timestamp()
                ),
            }
        # 合并df和pre_df，用于后续计算和更新。
        all_transactions = pd.concat([df, pre_df])
        all_transactions = all_transactions.drop_duplicates(subset=["transaction_hash"])
        # 将所有交易时间转换为上海时区。
        all_transactions["local_time"] = all_transactions["local_time"].dt.tz_convert(
            "Asia/Shanghai"
        )

        # 更新pre_edge_data中的统计信息。
        pre_edge_data["first_usdt_time"] = min(all_transactions["local_time"])
        pre_edge_data["last_usdt_time"] = max(all_transactions["local_time"])
        pre_edge_data["transcations_count"] = all_transactions.shape[0]
        pre_edge_data["usdt_amount"] = sum(all_transactions["amount"])

        # 筛选出df中不在pre_df中的交易，这些交易将被添加到图数据库中。
        if not pre_df.empty:
            hashs = set(pre_df["transaction_hash"])
            transactions = df[~df["transaction_hash"].isin(hashs)]
        else:
            transactions = df

        # 更新发送者的交易统计信息。
        pre_start_data["usdt_out_count"] = (
            pre_start_data["usdt_out_count"] + transactions.shape[0]
        )
        pre_start_data["usdt_out_amount"] = pre_start_data["usdt_out_amount"] + sum(
            transactions["amount"]
        )

        pre_start_data["usdt_out_start_time"] = min(
            pre_start_data["usdt_out_start_time"],
            int(pre_edge_data["first_usdt_time"].timestamp()),
        )
        pre_start_data["usdt_out_end_time"] = max(
            pre_start_data["usdt_out_end_time"],
            int(pre_edge_data["last_usdt_time"].timestamp()),
        )

        # 更新接收者的交易统计信息。
        pre_end_data["usdt_in_count"] = (
            pre_end_data["usdt_in_count"] + transactions.shape[0]
        )
        pre_end_data["usdt_in_amount"] = pre_end_data["usdt_in_amount"] + sum(
            transactions["amount"]
        )

        pre_end_data["usdt_in_start_time"] = min(
            int(pre_end_data["usdt_in_start_time"]),
            int(pre_edge_data["first_usdt_time"].timestamp()),
        )
        pre_end_data["usdt_in_end_time"] = max(
            int(pre_end_data["usdt_in_end_time"]),
            int(pre_edge_data["last_usdt_time"].timestamp()),
        )
        # 将更新后的发送者和接收者节点信息添加到Neo4j图数据库中。
        await self.neo4j.add_node(
            pre_start_address, **DataConverter.to_neo4j_properties(pre_start_data)
        )
        await self.neo4j.add_node(
            pre_end_address, **DataConverter.to_neo4j_properties(pre_end_data)
        )
        # 添加一条表示交易的边到Neo4j图数据库中，使用pre_edge_data中的信息。
        await self.neo4j.add_or_update_edge(
            df.head(1)["sender"].values[0],
            df.head(1)["receiver"].values[0],
            **DataConverter.to_neo4j_properties(pre_edge_data),
        )
        transactions_dict = defaultdict(list)


        for _, row in tqdm(transactions.iterrows()):
            center, address, in_out = await self.get_direction(
                row["sender"], row["receiver"]
            )
            center_id, address_id = await self.get_address_id(
                center
            ), await self.get_address_id(address)
            if row["sender"] == center:
                row["sender"] = center_id
                row["receiver"] = address_id
            elif row["sender"] == address:
                row["sender"] = address_id
                row["receiver"] = center_id

            pair_id, _ = await self.get_pair_id(center, address)
            filter_criteria = {"pair_id": pair_id, "in_out": in_out}
            transaction = dict(row)
            transaction.update(filter_criteria)
            transactions_dict[(pair_id, in_out)].append(transaction)

        # 使用bulk_write进行批量更新
        bulk_operations = []
        for key, transactions_list in transactions_dict.items():
            if transactions_list:
                filter_criteria = {"pair_id": key[0], "in_out": key[1]}
                update_operation = {"$push": {"transactions": {"$each": transactions_list}}}
                bulk_operations.append(
                    UpdateOne(filter_criteria, update_operation, upsert=True)
                )

        if bulk_operations:
            await self.transactions.bulk_write(bulk_operations)



# 使用示例
if __name__ == "__main__":

    async def main():
        bt = BlockchainTransactions()
        await bt.init_counter()
        # (
        #     await bt.get_all_usdt_trans_records(
        #         [
        #             "TTVk3xqKJZA3oVo3g91oirbifsUnJyYzdp",
        #             "TLRhiCwfqHbEdLqEDBsUA83JQPJAhyr7yQ",
        #             # "TPGZHxmFwHwNQHRHr5pb7SsdHJB4jVhi1k",
        #             # "TL7QUBTHo9tmqdzyDXod3ikQuLhH75QFAS",
        #             # "TNj3LgXWLMp1oVdfCNKFkPKfUrCCqftcGr",
        #             # "TWrxonF66TALQwJzPiJCqLstdZb3v533DY",
        #             # "TXLWDuAP3F7xVjqfZPwGrnvTBYHTGYT1Qu",
        #         ]
        #     )
        # ).to_csv("test2.csv")
        # print(1)
        (
            await bt.get_transactions(
                "TTVk3xqKJZA3oVo3g91oirbifsUnJyYzdp",
                "TSvudPTE2SC8X9AcGoBgiU1QqM3QjG6FR6",
            )
        ).to_csv("test.csv")
        print(2)
        await bt.close()

    asyncio.run(main())
