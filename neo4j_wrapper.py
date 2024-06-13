from neo4j import AsyncGraphDatabase, EagerResult, AsyncResult
from pandas import Timestamp


class DataConverter:
    @staticmethod
    def to_neo4j_properties(data):
        """
        将Python字典转换为Neo4j属性格式。
        只允许基本类型（int, float, str, bool）和它们的数组。
        """
        neo4j_data = {}
        for key, value in data.items():
            if isinstance(value, (int, str, bool, float)):
                neo4j_data[key] = int(value) if isinstance(value, int) else value
            elif isinstance(value, Timestamp):
                # 将Timestamp转换为int
                neo4j_data[key] = int(value.timestamp())
            elif isinstance(value, list) and all(
                isinstance(i, (int, float, str, bool)) for i in value
            ):
                neo4j_data[key] = [int(i) if isinstance(i, int) else i for i in value]
            else:
                raise ValueError(
                    f"Unsupported data type for Neo4j property: {type(value)}"
                )
        return neo4j_data

    @staticmethod
    def from_neo4j_properties(properties):
        """
        将Neo4j属性格式转换为Python字典。
        """
        return dict(properties)


class Neo4jWrapper:
    def __init__(self, url, user, password):
        self.driver = AsyncGraphDatabase.driver(url, auth=(user, password))
        self.session = self.driver.session()

    async def write(self, query):
        await self.session.write_transaction(self._write, query)

    @staticmethod
    async def _write(tx, query):
        await tx.run(query)

    async def add_node(self, node, **attr):
        async with self.driver.session() as session:
            await session.write_transaction(self._create_node, node, attr)
    @staticmethod
    async def _create_node(tx, node, attr):
        properties = DataConverter.to_neo4j_properties(attr)
        

        # 检查节点是否存在
        check_query = """
        MATCH (n:Node {id: $id})
        RETURN n
        """
        result = await tx.run(check_query, id=node)
        existing_node = await result.single()

        if existing_node:
            # 更新节点的属性
            update_query = """
            MATCH (n:Node {id: $id})
            SET n += $properties
            """
            await tx.run(update_query, id=node, properties=properties)
        else:
            # 构建动态的 Cypher 查询语句
            query = "CREATE (n:Node {id: $id"
            for key in properties.keys():
                query += f", {key}: ${key}"
            query += "})"

            # 将属性值添加到参数字典中
            parameters = {"id": node}
            parameters.update(properties)

            await tx.run(query, parameters)


    async def add_edge(self, node1, node2, **attr):
        async with self.driver.session() as session:
            await session.write_transaction(self._create_edge, node1, node2, attr)

    @staticmethod
    async def _create_edge(tx, node1, node2, attr):
        properties = DataConverter.to_neo4j_properties(attr)
        # 构建动态的 Cypher 查询语句
        query = """
        MATCH (a:Node {id: $node1}), (b:Node {id: $node2})
        CREATE (a)-[r:RELATIONSHIP {
        """
        for key in properties.keys():
            query += f"{key}: ${key}, "
        query = query.rstrip(", ")  # 移除最后一个逗号和空格
        query += "}]->(b)"

        # 将属性值添加到参数字典中
        parameters = {"node1": node1, "node2": node2}
        parameters.update(properties)

        await tx.run(query, parameters)

    async def add_or_update_edge(self, node1, node2, **attr):
        async with self.driver.session() as session:
            await session.write_transaction(
                self._add_or_update_edge, node1, node2, attr
            )

    @staticmethod
    async def _add_or_update_edge(tx, node1, node2, attr):
        properties = DataConverter.to_neo4j_properties(attr)

        # 检查边是否存在
        check_query = """
        MATCH (a:Node {id: $node1})-[r:RELATIONSHIP]->(b:Node {id: $node2})
        RETURN r
        """
        result = await tx.run(check_query, node1=node1, node2=node2)
        existing_edge = await result.single()

        if existing_edge:
            # 更新边的属性
            update_query = """
            MATCH (a:Node {id: $node1})-[r:RELATIONSHIP]->(b:Node {id: $node2})
            SET r += $properties
            """
            await tx.run(update_query, node1=node1, node2=node2, properties=properties)
        else:
            # 创建新的边
            create_query = """
            MATCH (a:Node {id: $node1}), (b:Node {id: $node2})
            CREATE (a)-[r:RELATIONSHIP]->(b)
            SET r = $properties
            """
            await tx.run(create_query, node1=node1, node2=node2, properties=properties)

    async def get_outgoing_edges(self, node_id):
        async with self.driver.session() as session:
            records = await session.read_transaction(self._get_outgoing_edges, node_id)
            edges_list = []
            for record in records.records:
                edge = record["e"]
                edge_properties = dict(edge.items())
                edges_list.append(edge_properties)

            return edges_list

    async def get_incoming_edges(self, node_id):
        async with self.driver.session() as session:
            records = await session.read_transaction(self._get_incoming_edges, node_id)
            edges_list = []
            for record in records.records:
                edge = record["e"]
                edge_properties = dict(edge.items())
                edges_list.append(edge_properties)

            return edges_list

    @staticmethod
    async def _get_outgoing_edges(tx, node_id):
        query = """
        MATCH (n)-[e]->() WHERE n.id = $node_id RETURN e
        """
        result: AsyncResult = await tx.run(query, node_id=node_id)
        return await result.to_eager_result()

    @staticmethod
    async def _get_incoming_edges(tx, node_id):
        query = """
        MATCH (n)<-[e]-() WHERE n.id = $node_id RETURN e
        """
        result = await tx.run(query, node_id=node_id)
        return await result.to_eager_result()

    async def nodes(self, node_ids):
        try:
            async with self.driver.session() as session:
                nodes_list = await session.read_transaction(self._get_nodes, node_ids)
                if not nodes_list:
                    return [{}]
                return nodes_list
        except Exception as e:
            print(e)

    @staticmethod
    async def _get_nodes(tx, node_ids):
        # 确保 node_ids 是一个列表
        if not isinstance(node_ids, list):
            node_ids = [node_ids]

        query = "MATCH (n:Node) WHERE n.id IN $node_ids RETURN n.id AS node_id, n"
        result = await tx.run(query, node_ids=node_ids)
        records = await result.to_eager_result()
        nodes_list = [
            {"node_id": record["node_id"], **record["n"]} for record in records.records
        ]
        return nodes_list

    async def edges(self, *node_ids):
        async with self.driver.session() as session:
            try:
                records = await session.read_transaction(
                    self._get_edges, list(node_ids)
                )
            except Exception as e:
                print(e)
                return []
            if not records[0]:
                return records
            return records

    @staticmethod
    async def _get_edges(tx, node_ids):
        results = []
        for node_id in node_ids:
            query = """
            MATCH (start_node:Node {id: $node_id})
            CALL apoc.path.expandConfig(start_node, {
                relationshipFilter: 'RELATIONSHIP>',
                labelFilter: '+Node',
                minLevel: 1,
                maxLevel: 5,
                bfs: true,
                filterStartNode: true
            }) YIELD path
            WITH path, nodes(path) AS nodes, relationships(path) AS rels
            WHERE ALL(node IN nodes WHERE node.id IN $node_ids)
            UNWIND rels AS r
            RETURN startNode(r).id AS start_node, endNode(r).id AS end_node, r AS r
            """
            result: EagerResult = await tx.run(
                query, node_id=node_id, node_ids=node_ids
            ).to_eager_result()
            # 组装
            for i, _ in enumerate(result.records):
                if not result.records[0]:
                    if i % 2 == 0 and i > 0:
                        results.append(
                            {
                                key: result.records[j]
                                for j, key in enumerate(result.records[i])
                            }
                        )
                else:
                    break
        return results

    @staticmethod
    async def _get_edges(tx, node_ids):
        results = []
        for node_id in node_ids:
            query = """
            MATCH (start_node:Node {id: $node_id})
            CALL apoc.path.expandConfig(start_node, {
                relationshipFilter: 'RELATIONSHIP>',
                labelFilter: '+Node',
                minLevel: 1,
                maxLevel: -1,
                bfs: true,
                filterStartNode: true
            }) YIELD path
            WITH path, nodes(path) AS nodes, relationships(path) AS rels
            WHERE ALL(node IN nodes WHERE node.id IN $node_ids)
            UNWIND rels AS r
            RETURN startNode(r).id AS start_node, endNode(r).id AS end_node, r AS r
            """
            result = await tx.run(query, node_id=node_id, node_ids=node_ids)
            results.extend(await result.to_eager_result())
        return results

    async def check_nodes_exist(self, *node_ids):
        async with self.driver.session() as session:
            existing_set = await session.read_transaction(
                self._check_nodes_exist, list(node_ids)
            )
            return existing_set

    @staticmethod
    async def _check_nodes_exist(tx, node_ids):
        query = """
        MATCH (n:Node)
        WHERE n.id IN $node_ids
        RETURN n.id AS id
        """
        records: EagerResult = await tx.run(query, node_ids=node_ids).to_eager_result()
        existing_set = {record["id"] for record in records.records}
        return existing_set


if __name__ == "__main__":

    async def main():
        nw = Neo4jWrapper(
            url="neo4j://127.0.0.1:7474", user="neo4j", password="N@eo4j852853"
        )
        result = await nw.get_outgoing_edges(
            node_id="TQ6wb5TLYZAZEBPSE9FJHDTfPx6KJzLkrb"
        )
        print(result)

    import asyncio

    asyncio.run(main())
