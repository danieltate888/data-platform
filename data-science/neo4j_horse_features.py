# from neo4j import GraphDatabase
# import pandas as pd
#
# # 连接Neo4j数据库
# driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "admin123"))
#
# def fetch_neo4j_features():
#     with driver.session() as session:
#
#         # 确保有向图存在，用于PageRank
#         session.run("""
#         CALL gds.graph.drop('horseGraph', false) YIELD graphName
#         """)
#         session.run("""
#         CALL gds.graph.project(
#           'horseGraph',
#           'Horse',
#           {
#             SIRE: { orientation: 'NATURAL' },
#             DAM: { orientation: 'NATURAL' }
#           }
#         )
#         """)
#
#         # PageRank查询
#         pagerank_query = """
#         CALL gds.pageRank.stream('horseGraph')
#         YIELD nodeId, score
#         RETURN gds.util.asNode(nodeId).id AS id, score AS pagerank
#         """
#         pagerank = pd.DataFrame(session.run(pagerank_query).data())
#
#         # Degree/InDegree/OutDegree查询（注意用COUNT {})
#         degree_query = """
#         MATCH (h:Horse)
#         RETURN h.id AS id,
#                COUNT { (h)--() } AS degree,
#                COUNT { (h)<--() } AS in_degree,
#                COUNT { (h)-->() } AS out_degree
#         ORDER BY id
#         """
#         degree = pd.DataFrame(session.run(degree_query).data())
#
#         # 重新建无向图，用于Clustering Coefficient
#         session.run("""
#         CALL gds.graph.drop('horseGraph', false) YIELD graphName
#         """)
#         session.run("""
#         CALL gds.graph.project(
#           'horseGraph',
#           'Horse',
#           {
#             SIRE: { orientation: 'UNDIRECTED' },
#             DAM: { orientation: 'UNDIRECTED' }
#           }
#         )
#         """)
#
#         # Clustering Coefficient查询
#         cluster_query = """
#         CALL gds.localClusteringCoefficient.stream('horseGraph')
#         YIELD nodeId, localClusteringCoefficient
#         RETURN gds.util.asNode(nodeId).id AS id, localClusteringCoefficient AS clustering_coefficient
#         ORDER BY id
#         """
#         clustering = pd.DataFrame(session.run(cluster_query).data())
#
#         # 合并特征表
#         features = pagerank.merge(degree, on='id').merge(clustering, on='id')
#
#         return features
#
# # 调用
# neo4j_features = fetch_neo4j_features()
# print(neo4j_features.head())
#
# # 可以直接保存成CSV
# output_file = "neo4j_horse_features.csv"
# neo4j_features.to_csv(output_file, index=False)
# print(f"✅ Feature table has been successfully saved to '{output_file}'.")

from neo4j import GraphDatabase
import pandas as pd

# 连接Neo4j
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "admin123"))

def fetch_neo4j_features():
    with neo4j_driver.session() as session:
        session.run("CALL gds.graph.drop('horseGraph', false)")
        session.run("""
            CALL gds.graph.project(
              'horseGraph',
              'Horse',
              {
                SIRE: { orientation: 'NATURAL' },
                DAM: { orientation: 'NATURAL' }
              }
            )
        """)
        pagerank = pd.DataFrame(session.run("""
            CALL gds.pageRank.stream('horseGraph')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).id AS id, score AS pagerank
        """).data())

        degree = pd.DataFrame(session.run("""
            MATCH (h:Horse)
            RETURN h.id AS id,
                   COUNT { (h)--() } AS degree,
                   COUNT { (h)<--() } AS in_degree,
                   COUNT { (h)-->() } AS out_degree
        """).data())

        session.run("CALL gds.graph.drop('horseGraph', false)")
        session.run("""
            CALL gds.graph.project(
              'horseGraph',
              'Horse',
              {
                SIRE: { orientation: 'UNDIRECTED' },
                DAM: { orientation: 'UNDIRECTED' }
              }
            )
        """)

        clustering = pd.DataFrame(session.run("""
            CALL gds.localClusteringCoefficient.stream('horseGraph')
            YIELD nodeId, localClusteringCoefficient
            RETURN gds.util.asNode(nodeId).id AS id, localClusteringCoefficient AS clustering_coefficient
        """).data())

        features = pagerank.merge(degree, on='id').merge(clustering, on='id')
        return features

if __name__ == "__main__":
    neo4j_features = fetch_neo4j_features()
    neo4j_features.to_csv("neo4j_horse_features.csv", index=False)
    print("✅ Neo4j features saved.")