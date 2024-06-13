# OKLink
from sklearn.metrics.pairwise import polynomial_kernel
import networkx as nx
# 基于污点传播的图搜索
import numpy as np
import os
import pandas as pd
import pymongo
from PriorityQueue import PriorityQueueClient
from TagCreeper import TagCreeper
from TestTrxAddress import TrxAddress, get_first_trx_min_common_level_numpy
from concurrent.futures import ThreadPoolExecutor
# from multiprocessing import Pool, Manager
from functools import partial
from pandarallel import pandarallel
import random


def concat_df(edge_dfs, node_dfs):
    node_dfs = pd.concat(node_dfs)
    edge_dfs = pd.concat(edge_dfs)
    # 聚合
    edge_dfs = (
        edge_dfs.groupby(["src", "dst"])
        .agg({"amount": "max", "count": "max"})
        .reset_index()
    )
    node_dfs = node_dfs.drop_duplicates(subset=["id"])
    # 保存
    return node_dfs, edge_dfs


def get_score(nodes_df, rank1_end, rank2_end, addresses_df):
    # 计算addresses_df中每个地址在nodes_data中的排名，地址可分为三类，需要输入三类地址的排名起始位置，第一类的评分为3*排名，第二类的评分为2*排名，第三类的评分为1*排名
    # 计算nodes_df

    score2 = []
    # 创建无向图
    nodes_df = nodes_df.sort_values(by=["tainted_weight"])
    nodes_df = nodes_df.reset_index(drop=True)
    # 取出第一类地址的列表
    rank1 = list(addresses_df.iloc[0:rank1_end]["address"])
    # 取出第二类地址的列表
    rank2 = list(addresses_df.iloc[rank1_end:rank2_end]["address"])
    # 取出第三类地址的列表
    rank3 = list(addresses_df.iloc[rank2_end:]["address"])

    scores = []

    for address in rank1:
        # 如果在nodes_df中找不到该地址
        if address not in list(nodes_df["id"]):
            # 计算图G中目标节点到所有已探索节点的最短路径的平均长度
            pass
        else:
            scores.append(
                -3
                * (
                        nodes_df.shape[0] / nodes_df[nodes_df["id"] == address].index[0]
                )
            )
            score2.append(3)

    for address in rank2:
        # 如果在nodes_df中找不到该地址
        if address not in list(nodes_df["id"]):
            pass
        else:
            scores.append(
                -2
                * (
                        nodes_df.shape[0] / nodes_df[nodes_df["id"] == address].index[0]
                )
            )
            score2.append(2)

    for address in rank3:
        # 如果在nodes_df中找不到该地址
        if address not in list(nodes_df["id"]):
            pass
        else:
            scores.append(
                -1
                * (
                        nodes_df.shape[0] / nodes_df[nodes_df["id"] == address].index[0]
                )
            )
            score2.append(1)
    return sum(scores) * (len(score2) ** 3), score2


def get_score2(rank1_end, rank2_end, addresses_df, nodes_df):
    # 计算addresses_df中每个地址在nodes_data中的排名，地址可分为三类，需要输入三类地址的排名起始位置，第一类的评分为3*排名，第二类的评分为2*排名，第三类的评分为1*排名
    # 计算nodes_df
    # print(nodes_df)

    # 读取总图
    edges_df = pd.read_csv("新币_edges.csv")
    # 创建无向图

    nodes_df = nodes_df.sort_values(by=["tainted_weight"])
    nodes_df = nodes_df.reset_index(drop=True)
    # 取出第一类地址的列表
    rank1 = list(addresses_df.iloc[0:rank1_end]["address"])
    # 取出第二类地址的列表
    rank2 = list(addresses_df.iloc[rank1_end:rank2_end]["address"])
    # 取出第三类地址的列表
    rank3 = list(addresses_df.iloc[rank2_end:]["address"])

    G = nx.from_pandas_edgelist(
        edges_df,
        source="src",
        target="dst",
        create_using=nx.Graph(),
    )

    scores = []

    def average_shortest_path_length(full_G, source_node, target_address):
        total_length = []
        for target_node in target_address:
            total_length.append(nx.shortest_path_length(full_G, source_node, target_node))
        return sum(total_length) / len(total_length)

    def min_shortest_path_length(full_G, source_node, target_addresses):
        all_lengths = []
        for target_node in target_addresses:
            all_lengths.append(nx.shortest_path_length(full_G, source_node, target_node))
        return min(all_lengths)

    explored_nodes = list(nodes_df[nodes_df["tag"] != "未检查tag"]["id"])
    for address in rank1:
        # 如果在nodes_df中找不到该地址
        if address not in list(nodes_df["id"]):
            # 计算图G中目标节点到所有已探索节点的最短路径的平均长度
            path_length = min_shortest_path_length(G, address, explored_nodes)
            scores.append(
                3 * path_length
            )
            # print(min_shortest_path_length(G, address, target_nodes))
        else:
            scores.append(
                1
                * (
                        nodes_df[nodes_df["id"] == address].index[0]
                        / nodes_df.shape[0]
                )
            )

    for address in rank2:
        # 如果在nodes_df中找不到该地址
        if address not in list(nodes_df["id"]):
            path_length = min_shortest_path_length(G, address, explored_nodes)
            scores.append(
                2 * path_length
            )
        else:
            scores.append(
                2
                * (
                        nodes_df[nodes_df["id"] == address].index[0]
                        / nodes_df.shape[0]
                )
            )

    for address in rank3:
        # 如果在nodes_df中找不到该地址
        if address not in list(nodes_df["id"]):
            path_length = min_shortest_path_length(G, address, explored_nodes)
            scores.append(
                1 * path_length
            )
        else:
            scores.append(
                3
                * (
                        nodes_df[nodes_df["id"] == address].index[0]
                        / nodes_df.shape[0]
                )
            )
    return sum(scores)


class TronGraph:
    def __init__(self, W, gamma=0.1, coef0=0.0) -> None:
        """维持三个图，usdt交易图，trx交易图，gas激活图"""
        self.usdt_graph = nx.DiGraph()
        self.trx_graph = nx.DiGraph()
        self.usdt_tainted_dict = {}
        # gas激活图用一个字典表示，key是地址，value是一个列表，列表是该地址的激活路径
        self.gas_graph_dict = {}
        self.W = W
        self.gamma = gamma
        self.coef0 = coef0
        self.redis_id = str(random.randint(0, 10000))

    def handle_gas_graph(self, db, x):
        if x not in self.gas_graph_dict.keys():
            try:
                self.gas_graph_dict[x] = TrxAddress(x).get_first_trx_chain_for_tron_graph(
                    db=db,
                    is_not_filter=True,
                    is_not_stop=True,
                )
            except Exception as e:
                print("An exception occurred:", str(e))

    def explore_illegal_routes(
            self,
            start_address,
            tainted_weight,
            max_nodes,
            is_sell,
            is_filter,
    ):
        """
        开始结点是一个违法的地址,它有一个污点权重,比如说是1000.
        根据违法地址的卖出边数额大小和次数来计算边的终点的污点权重,污点权重值高地优先探索.
        这个污点权重由上一个点的污点值和卖出边的数额大小和次数所得到的污点权重来决定.
        Args:
            start_address (_type_): 查找开始的地址
            tainted_weight (_type_): 初始的污点权重
            max_nodes (_type_): 结点数量限制

        Returns:
            _type_: 返回结点和边的信息
        """

        def add_to_trx_graph(address):
            """
            宽度优先遍历,从当前结点开始，获取每一个节点的所有出边和入边，遍历下一层结点,直到遍历完所有结点或者达到层数限制
            """

            # 把所有边添加到trx_graph中
            def add_edges_to_trx_graph(edges):
                for _, row in edges.iterrows():
                    source, target, amount, count = (
                        row["src"],
                        row["dst"],
                        row["amount"],
                        row["count"],
                    )
                    # 添加到trx_graph
                    self.trx_graph.add_edge(source, target, amount=amount, count=count)

            def bfs(address, only_form, only_to):
                start_address = TrxAddress(address)
                # 获取第一个节点的所有出边或入边
                edges = start_address.get_group_trade(
                    is_trx=False,
                    start_or_end_time=0,
                    only_form=only_form,
                    only_to=only_to,
                )
                # 过滤所有amount小于100的边
                edges = edges[edges["amount"] > 100]
                add_edges_to_trx_graph(edges)
                for _, row in edges.iterrows():
                    if only_form is True and only_to is False:
                        source, target, timestamp = (
                            row["src"],
                            row["dst"],
                            row["timestamp"],
                        )
                        # 对target进行探索，并把target的出边添加到trx_graph中
                        start_address = TrxAddress(target)
                    else:
                        source, target, timestamp = (
                            row["dst"],
                            row["src"],
                            row["timestamp"],
                        )
                        # 对source进行探索，并把source的入边添加到trx_graph中
                        start_address = TrxAddress(source)

                    edges = start_address.get_group_trade(
                        is_trx=False,
                        start_or_end_time=timestamp,
                        only_form=only_form,
                        only_to=only_to,
                    )
                    edges = edges[edges["amount"] > 100]
                    add_edges_to_trx_graph(edges)

            # 遍历卖出边
            bfs(address, only_form=True, only_to=False)
            # 遍历卖入边
            bfs(address, only_form=False, only_to=True)

        def get_edges_data(curr_address, start_or_end_time):
            """
            Retrieves edgese data based on the given current address and start or end tim.


            Parameters:
                curr_address (str): The current address.
                start_or_end_time (int): The start or end time.

            Returns:
                str: Returns "无交易信息" if the edges dataframe is empty. Otherwise, returns "交易收集成功".
            """
            # print(curr_address, end="\t")
            trx_address = TrxAddress(curr_address)
            if is_sell:
                edges_df = trx_address.get_group_trade(
                    is_trx=False, start_or_end_time=start_or_end_time, only_form=True
                )  # 获取卖出边信息
            else:
                edges_df = trx_address.get_group_trade(
                    is_trx=False, start_or_end_time=start_or_end_time, only_to=True
                )  # 获取卖入边信息
            time_print = pd.Timestamp(
                start_or_end_time / 1000, unit="s", tz="Asia/Shanghai"
            )
            # print("从", time_print, "开始或截止的交易对手数目为", len(edges_df))

            if edges_df.empty:
                return "无交易信息"

            # 得到边的usdt污点权重
            edges_df["weight"] = edges_df["amount"] / edges_df["amount"].sum()
            # 把信息添加到图中
            for _, row in edges_df.iterrows():
                source, target, amount, count, timestamp = (
                    row["src"],
                    row["dst"],
                    row["amount"],
                    row["count"],
                    row["timestamp"],
                )
                # 如果amount==0,则不添加边min
                if amount == 0:
                    continue

                if target not in visited:
                    # 寻找dict中是否有targe,更新usdt_tainted_weight
                    if self.usdt_tainted_dict.get(source) is None:
                        pres = list(self.usdt_graph.predecessors(source))
                        if not pres:
                            self.usdt_tainted_dict[source] = 0
                        else:
                            if self.usdt_tainted_dict.get(target) is None:
                                self.usdt_tainted_dict[target] = 0
                            for pre in pres:
                                out_edges = self.usdt_graph.out_edges(pre, data=True)
                                total_weight = sum(data['amount'] for _, _, data in out_edges)
                                target_weight = self.usdt_graph.get_edge_data(pre, target)["amount"]
                                ratio = target_weight / total_weight
                                self.usdt_tainted_dict[target] += self.usdt_tainted_dict[pre] * ratio
                        print(self.usdt_tainted_dict[source])

                    if target not in self.usdt_tainted_dict:
                        self.usdt_tainted_dict[target] = (
                                self.usdt_tainted_dict[source] * row["weight"]
                        )
                    else:
                        self.usdt_tainted_dict[target] += (
                                self.usdt_tainted_dict[source] * row["weight"]
                        )
                # 添加到dataframe中
                nonlocal edges_data
                edges_data.append(
                    {
                        "src": source,
                        "dst": target,
                        "amount": amount,
                        "count": count,
                        "timestamp": timestamp,
                    }
                )  # type:ignore
                # 添加到usdt图中
                self.usdt_graph.add_edge(source, target, amount=amount, count=count)
                self.usdt_graph.add_node(
                    target, tainted_weight=self.usdt_tainted_dict[target]
                )

            # # 取出所有amount前20%的边的dst列表, 对它们进行trx激活链路的查询
            dst_addresses = edges_df[edges_df["amount"] > edges_df["amount"].quantile(0.8)]["dst"].unique()

            client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
            db = client["trx_gas"]
            pool = ThreadPoolExecutor(max_workers=16)
            pool.map(
                partial(self.handle_gas_graph, db),
                dst_addresses,
            )
            pool.shutdown()

            # dst_addresses = edges_df[edges_df["amount"] > edges_df["amount"].quantile(0.8)]["dst"].unique()
            #
            # client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
            # db = client["trx_gas"]
            #
            # for address in dst_addresses:
            #     self.handle_gas_graph(db, address)

            def handle_address(x):
                # print(x)
                address_and_timestamp = (target_address, target_timestamp) = x["dst"], x["timestamp"]
                """
                探索方式的不同
                """
                new_tainted_weight = get_tainted_weight(target_address)
                # new_tainted_weight = self.usdt_tainted_dict[target_address]
                # 改变target的污点权重
                self.usdt_graph.nodes[target_address]["tainted_weight"] = new_tainted_weight
                # # for adam
                # self.tensor_dict[target_address] = new_tainted_weight

                if target_address not in visited:
                    # 更新target的污点权重，如果没有target就创建一个
                    _priority_queue_client = PriorityQueueClient(self.redis_id)
                    _priority_queue_client.enqueue(address_and_timestamp, new_tainted_weight)

            # 获得每一个地址的污点权重，然后放到堆中
            edges_df.parallel_apply(
                handle_address, axis=1
            )

            return "交易收集成功"

        def get_tainted_weight(address):
            """将需要的特征提取出来，放到一个dataframe里"""
            # 1，获取父节点数量
            neighbors = list(self.usdt_graph.predecessors(address))
            num_neighbors = len(neighbors)
            # 2，获取父结点的污点权重总和，平均值，标准差
            weights = np.fromiter((self.usdt_graph.nodes[neighbor]["tainted_weight"] for neighbor in neighbors), float,
                                  num_neighbors)
            (
                sum_weight,
                avg_weight,
                # std_weight,
            ) = (
                np.sum(weights),
                np.mean(weights),
                # np.std(weights),
            )
            # 3，获取父节点的usdt污点权重总和，平均值，和标准差
            usdt_weights = np.fromiter((self.usdt_tainted_dict[neighbor] for neighbor in neighbors), float,
                                       num_neighbors)
            (
                sum_usdt_weight,
                avg_usdt_weight,
                # std_usdt_weight,
            ) = (
                np.sum(usdt_weights),
                np.mean(usdt_weights),
                # np.std(usdt_weights),
            )

            # 4,获取所有终点为address的边的amount属性，count属性的总和，最小值，最大值，平均值，中位数，和标准差
            edges = self.usdt_graph.in_edges(address)

            amounts = np.fromiter((self.usdt_graph[edge[0]][address]["amount"] for edge in edges), float, num_neighbors)
            counts = np.fromiter((self.usdt_graph[edge[0]][address]["count"] for edge in edges), float, num_neighbors)
            (
                sum_amount,
                avg_amount,
                # std_amount,
            ) = (
                np.sum(amounts),
                np.mean(amounts),
                # np.std(amounts),
            )
            (
                sum_count,
                avg_count,
                # std_count
            ) = (
                np.sum(counts),
                np.mean(counts),
                # np.std(counts),
            )
            # 5，获取相同gas激活路径的地址的信息
            gas_path = self.gas_graph_dict.get(address, [])
            # print(gas_path.__len__())
            # 找到gas激活兄弟节点的信息
            gas_brother_df = []
            if gas_path:
                for path in self.gas_graph_dict.values():
                    if path != gas_path:
                        level = get_first_trx_min_common_level_numpy(
                            path, gas_path
                        )

                        # 有共同gas激活祖先
                        if level < 999:
                            brother_address = path[0]
                            # 获取brother_address的污点权重
                            # print(path[0], gas_path[0])
                            brother_tainted_weight = self.usdt_graph.nodes[
                                brother_address
                            ]["tainted_weight"]
                            # 获取brother_address的usdt污点权重
                            brother_usdt_weight = self.usdt_tainted_dict[
                                brother_address
                            ]
                            # 放入dataframe
                            gas_brother_df.append(
                                {
                                    "level": level,
                                    "brother_tainted_weight": brother_tainted_weight,
                                    "brother_usdt_weight": brother_usdt_weight,
                                    "level*brother_usdt_weight": (9 - level) * brother_usdt_weight,
                                    "level*brother_tainted_weight": (9 - level) * brother_tainted_weight,
                                },
                            )  # type:ignore
                        # 否则跳过
                        else:
                            continue
            if not gas_brother_df:
                #     # 全部列赋值0
                #     print("没有相同的gas激活路径")
                gas_brother_df.append(
                    {
                        "level": 0,
                        "brother_tainted_weight": 0,
                        "brother_usdt_weight": 0,
                        "level*brother_usdt_weight": 0,
                        "level*brother_tainted_weight": 0,
                    },
                )  # type:ignore

            gas_brother_df: pd.DataFrame = pd.DataFrame(gas_brother_df)
            # 对brother_df的每一列求总和，最小值，最大值，平均值，中位数，和标准差
            avg_level = gas_brother_df["level"].mean()
            (
                sum_brother_tainted_weight,
                avg_brother_tainted_weight,
                # std_brother_tainted_weight,
            ) = (
                gas_brother_df["brother_tainted_weight"].sum(),
                gas_brother_df["brother_tainted_weight"].mean(),
                # gas_brother_df["brother_tainted_weight"].std(),
            )
            (
                sum_brother_usdt_weight,
                avg_brother_usdt_weight,
                # std_brother_usdt_weight,
            ) = (
                gas_brother_df["brother_usdt_weight"].sum(),
                gas_brother_df["brother_usdt_weight"].mean(),
                # gas_brother_df["brother_usdt_weight"].std(),
            )
            (
                sum_level_brother_tainted_weight,
                avg_level_brother_tainted_weight,
                # std_level_brother_tainted_weight,
            ) = (
                gas_brother_df["level*brother_tainted_weight"].sum(),
                gas_brother_df["level*brother_tainted_weight"].mean(),
                # gas_brother_df["level*brother_tainted_weight"].std(),
            )
            (
                sum_level_brother_usdt_weight,
                avg_level_brother_usdt_weight,
                # std_level_brother_usdt_weight,
            ) = (
                gas_brother_df["level*brother_usdt_weight"].sum(),
                gas_brother_df["level*brother_usdt_weight"].mean(),
                # gas_brother_df["level*brother_usdt_weight"].std(),
            )
            # 把所有的特征放到一个nparray中, 并计算评分
            features = np.array(
                [
                    num_neighbors,
                    self.usdt_tainted_dict[address],
                    sum_weight,
                    avg_weight,
                    # std_weight,
                    sum_usdt_weight,
                    avg_usdt_weight,
                    # std_usdt_weight,
                    sum_amount,
                    avg_amount,
                    # std_amount,
                    sum_count,
                    avg_count,
                    # std_count,
                    sum_brother_tainted_weight,
                    avg_brother_tainted_weight,
                    # std_brother_tainted_weight,
                    sum_brother_usdt_weight,
                    avg_brother_usdt_weight,
                    # std_brother_usdt_weight,
                    avg_level,
                    sum_level_brother_tainted_weight,
                    avg_level_brother_tainted_weight,
                    # std_level_brother_tainted_weight,
                    sum_level_brother_usdt_weight,
                    avg_level_brother_usdt_weight,
                    # std_level_brother_usdt_weight,
                ]
            )
            # # 非多项式
            # # 如果有元素为na，则填为零
            # features = np.nan_to_num(features)
            # features = features / 100000
            # features = np.nan_to_num(features)
            # # features = features[:, np.newaxis]
            # kernel_value = np.dot(features, self.W)
            # # print(kernel_value)
            # return kernel_value

            # 计算多项式核函数值
            degree = 2
            try:
                kernel_value = polynomial_kernel(
                    features.reshape(1, -1),
                    self.W.reshape(1, -1),
                    degree=degree,
                    coef0=self.coef0,
                    gamma=self.gamma,
                )
                return kernel_value[0][0]
            except Exception as e:
                return 999999999999

            # # adam
            # import torch
            # features = torch.tensor(features / 100000, dtype=torch.float)
            # # 与self.W点乘
            # kernel_value = torch.dot(features, self.W)
            # # print(kernel_value)
            # return kernel_value

        # tag_creeper = TagCreeper()
        priority_queue_client: PriorityQueueClient = PriorityQueueClient(self.redis_id)
        # wrong_addresses = input("请输入oklink标注错误的地址，用空格分隔:").split()
        wrong_addresses = []
        pandarallel.initialize(progress_bar=False)
        visited = set()  # 已访问的结点
        weight = tainted_weight * max_nodes  # 初始污点权重
        priority_queue_client.clear()
        priority_queue_client.enqueue((start_address, 0), weight)
        self.usdt_tainted_dict = {start_address: weight}  # 初始化usdt_tainted_dict
        # print(weight)
        nodes_data = []  # 结点信息
        edges_data = []  # 边信息

        while len(visited) < max_nodes and not priority_queue_client.is_empty():
            (curr_address, start_or_end_time), curr_tainted_weight = priority_queue_client.dequeue()
            # 恢复为正数
            # tag = tag_creeper.search(curr_address)
            tag = "无标签"
            # 如果是交易所地址,则不继续探索
            if (
                    is_filter
                    and tag != "无标签"
                    and tag != "Gambling"
                    and (curr_address not in wrong_addresses)
                    and (curr_address not in visited)
            ):
                visited.add(curr_address)
                max_nodes += 1
                nodes_data.append(
                    {
                        "id": curr_address,
                        "tainted_weight": curr_tainted_weight,
                        "tag": tag,
                        "start_time": start_or_end_time,
                    }
                )  # type:ignore
                self.usdt_graph.add_node(
                    curr_address, tainted_weight=curr_tainted_weight, tag=tag
                )
                continue
            visited.add(curr_address)
            nodes_data.append(
                {
                    "id": curr_address,
                    "tainted_weight": curr_tainted_weight,
                    "tag": tag,
                    "start_time": start_or_end_time,
                }
            )  # type:ignore
            # print(curr_tainted_weight, curr_address)
            self.usdt_graph.add_node(
                curr_address, tainted_weight=curr_tainted_weight, tag=tag
            )
            # 遍历出边，更新各个图
            get_edges_data(curr_address, start_or_end_time)
            # 根据len(visited)和max_node生成进度条
            # print(f"进度:{len(visited)}/{max_nodes}", end="\r")
        # 将priority_queue中剩余的结点添加到nodes_data中
        while not priority_queue_client.is_empty():
            (curr_address, start_or_end_time), curr_tainted_weight = priority_queue_client.dequeue()
            # 取前50个结点获得tag
            tag = "未检查tag"
            nodes_data.append(
                {
                    "id": curr_address,
                    "tainted_weight": curr_tainted_weight,
                    "tag": tag,
                    "start_time": start_or_end_time,
                }
            )  # type:ignore
            self.usdt_graph.add_node(
                curr_address, tainted_weight=curr_tainted_weight, tag=tag
            )
        edges_data = pd.DataFrame(edges_data)
        # print(edges_data)
        edges_data = (
            edges_data.groupby(["src", "dst"])
            .agg({"amount": "sum", "count": "sum", "timestamp": "min"})
            .reset_index()
        )
        nodes_data = pd.DataFrame(nodes_data)
        nodes_data = nodes_data.drop_duplicates(subset=["id"])
        print(nodes_data.shape)

        # tag_creeper.close()
        priority_queue_client.clear()
        return nodes_data, edges_data

    def get_paths(self, start_address, tainted_weight, max_nodes, end_address):
        if f"{start_address}_sell_nodes.csv" not in os.listdir():
            # 开始点探索卖出边图
            nodes_df1, edges_df1 = self.explore_illegal_routes(
                start_address=start_address,
                tainted_weight=tainted_weight,
                max_nodes=max_nodes,
                is_sell=True,
                is_filter=True,
            )
            nodes_df1.to_csv(f"{start_address}_sell_nodes.csv", index=False)
            edges_df1.to_csv(f"{start_address}_sell_edges.csv", index=False)
        else:
            nodes_df1 = pd.read_csv(f"{start_address}_sell_nodes.csv")
            edges_df1 = pd.read_csv(f"{start_address}_sell_edges.csv")
        if f"{end_address}_buy_nodes.csv" not in os.listdir():
            # 结束点探索买入边图
            nodes_df2, edges_df2 = self.explore_illegal_routes(
                start_address=end_address,
                tainted_weight=tainted_weight,
                max_nodes=max_nodes,
                is_sell=False,
                is_filter=True,
            )
            nodes_df2.to_csv(f"{end_address}_buy_nodes.csv", index=False)
            edges_df2.to_csv(f"{end_address}_buy_edges.csv", index=False)
        else:
            nodes_df2 = pd.read_csv(f"{end_address}_buy_nodes.csv")
            edges_df2 = pd.read_csv(f"{end_address}_buy_edges.csv")
        # 合并两个图
        nodes_df = pd.concat([nodes_df1, nodes_df2])
        edges_df = pd.concat([edges_df1, edges_df2])

        # 聚合
        edges_df = (
            edges_df.groupby(["src", "dst"])
            .agg({"amount": "max", "count": "max"})
            .reset_index()
        )
        nodes_df = nodes_df.drop_duplicates(subset=["id"])
        return nodes_df, edges_df

    def get_graph_df(self, addresses, tainted_weight, max_nodes):
        """获取"""
        node_dfs = []
        edge_dfs = []
        for address in addresses:
            if f"{address}_sell_nodes.csv" not in os.listdir():
                # 开始点探索卖出边图
                nodes_df, edges_df = self.explore_illegal_routes(
                    start_address=address,
                    tainted_weight=tainted_weight,
                    max_nodes=max_nodes,
                    is_sell=True,
                    is_filter=True,
                )
                nodes_df.to_csv(f"{address}_sell_nodes.csv", index=False)
                edges_df.to_csv(f"{address}_sell_edges.csv", index=False)
                print(4)
                node_dfs.append(nodes_df)
                edge_dfs.append(edges_df)
                print(5)
            else:
                nodes_df = pd.read_csv(f"{address}_sell_nodes.csv")
                edges_df = pd.read_csv(f"{address}_sell_edges.csv")
                node_dfs.append(nodes_df)
                edge_dfs.append(edges_df)
        return concat_df(edge_dfs, node_dfs)


if __name__ == "__main__":
    # # start_address = 'TGvAUnLAmP21xFUk5vACmFWSZg5iNnczr1'  # 起始地址
    # start_address = 'TWrxonF66TALQwJzPiJCqLstdZb3v533DY'
    # end_address = 'TTn2G7RQHmA3D6sjBHXGuS5NH4Jx7Te5Ni'
    # tainted_weight = 1000  # 起始污点权重
    # max_nodes = 10  # 最大节点数
    # node_df, edge_df = get_paths(api, url, method, chainShortName,
    #                              tokenContractAddress, start_address,
    #                              tainted_weight, max_nodes, end_address)
    # node_df.to_csv(f'nodes.csv', index=False)
    # edge_df.to_csv(f'edges.csv', index=False)

    # -0.01到0.01的(1,19)向量
    # W = np.random.uniform(-0.000001, 0.000001, size=(19, ))
    # W = np.ones((19,))
    W = np.array(
        [-0.06376446, 0.00642685, -0.03161641, 0.02644675, 0.0733511, 0.0050923, -0.00576532, 0.02093931, 0.07396938,
         0.1, 0.04689537, -0.07767882, -0.09758978, 0.06777194, 0.0645549, 0.05873585, -0.04266283, 0.04135092])
    gamma = 0
    addresses = "TP5Ct1fCmhRConWimVpx4U9MwiMuExi9m2"
    from pyinstrument import Profiler

    profiler = Profiler()
    profiler.start()

    graph = TronGraph(W=W, gamma=1.53254512, coef0=0.1)
    nodes_df, _ = graph.explore_illegal_routes(
        start_address=addresses,
        tainted_weight=10000,
        max_nodes=75,
        is_sell=True,
        is_filter=True,
    )
    score1, node_list = get_score(
        rank1_end=14,
        rank2_end=27,
        addresses_df=pd.read_csv("address.csv"),
        nodes_df=nodes_df
    )
    score2 = get_score2(
        rank1_end=14,
        rank2_end=27,
        addresses_df=pd.read_csv("address.csv"),
        nodes_df=nodes_df
    )
    print("X:", W)
    print(node_list)
    print("score1:", score1)
    print("score2:", score2)
    profiler.stop()
    profiler.print()

    # node_df.to_csv("新币_test_nodes.csv", index=False)
    # edge_df.to_csv("新币_test_edges.csv", index=False)
