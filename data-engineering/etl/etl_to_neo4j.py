import os
from neo4j import GraphDatabase
from pyspark.sql import SparkSession

# 1. 初始化SparkSession
def init_spark():
    spark = SparkSession.builder \
        .appName("Load Silver Horses to Neo4j") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "admin123")
    hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    return spark

# 2. 连接Neo4j

def connect_neo4j():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "admin123"
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

# 3. 批量插入数据到Neo4j

def create_nodes_batch(tx, batch):
    query = """
    UNWIND $batch AS horse
    MERGE (h:Horse {id: horse.id})
    SET h.name = horse.name,
        h.gender = horse.gender,
        h.birth_year = horse.birth_year

    WITH h, horse
    CALL {
        WITH horse, h
        MATCH (sire:Horse {name: horse.sire_name})
        MERGE (h)-[:SIRE]->(sire)
        RETURN 0 AS sire_created
    }
    CALL {
        WITH horse, h
        MATCH (dam:Horse {name: horse.dam_name})
        MERGE (h)-[:DAM]->(dam)
        RETURN 0 AS dam_created
    }
    RETURN count(h)
    """
    tx.run(query, batch=batch)


def insert_horses_to_neo4j(driver, horses, batch_size=100):
    with driver.session() as session:
        for i in range(0, len(horses), batch_size):
            batch = horses[i:i+batch_size]
            session.execute_write(create_nodes_batch, batch)

# 4. 主流程

def main():
    spark = init_spark()

    silver_df = spark.read.format("delta").load("s3a://pedigrees360-datalake/horse-delta/silver-horses")

    horses = silver_df.select("id", "name", "gender", "birth_year", "sire_name", "dam_name") \
        .fillna({"sire_name": "", "dam_name": ""}) \
        .collect()

    horse_list = []
    for row in horses:
        horse_list.append({
            "id": int(row["id"]),
            "name": row["name"],
            "gender": row["gender"],
            "birth_year": int(row["birth_year"]),
            "sire_name": row["sire_name"],
            "dam_name": row["dam_name"],
        })

    driver = connect_neo4j()
    insert_horses_to_neo4j(driver, horse_list)
    driver.close()

    print("\n\u2705 Silver Horses has been loaded to Neo4j!\n")

if __name__ == "__main__":
    main()