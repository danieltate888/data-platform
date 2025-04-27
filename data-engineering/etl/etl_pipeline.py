# import os
# import psycopg2
# import boto3
# import pyarrow.parquet as pq
# import pyarrow as pa
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, trim, upper
#
# # 1. 从 PostgreSQL 导出数据到 MinIO S3
#
# def export_postgres_to_s3():
#     conn = psycopg2.connect(
#         host="localhost",
#         port=5434,
#         user="admin",
#         password="admin123",
#         dbname="horses"
#     )
#     cursor = conn.cursor()
#     cursor.execute('SELECT id, name, gender, birth_year, sire_name, dam_name FROM horses')
#     rows = cursor.fetchall()
#
#     schema = pa.schema([
#         ('id', pa.int64()),
#         ('name', pa.string()),
#         ('gender', pa.string()),
#         ('birth_year', pa.int32()),
#         ('sire_name', pa.string()),
#         ('dam_name', pa.string()),
#     ])
#
#     table = pa.Table.from_pylist([{
#         "id": r[0],
#         "name": r[1],
#         "gender": r[2],
#         "birth_year": r[3],
#         "sire_name": r[4],
#         "dam_name": r[5]
#     } for r in rows], schema=schema)
#
#     pq.write_table(table, 'horses.parquet')
#     print("\u2705 Parquet file created locally.")
#
#     s3 = boto3.client(
#         's3',
#         endpoint_url='http://localhost:9000',
#         aws_access_key_id='admin',
#         aws_secret_access_key='admin123',
#         region_name='us-east-1'
#     )
#
#     with open('horses.parquet', 'rb') as f:
#         s3.upload_fileobj(f, 'pedigrees360-datalake', 'horse-delta/horses.parquet')
#
#     print("\u2705 Parquet file uploaded to MinIO S3.")
#
#     cursor.close()
#     conn.close()
#
# # 2. 初始化 SparkSession
#
# def init_spark():
#     spark = SparkSession.builder \
#         .appName("Pedigrees360 ETL Pipeline") \
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#         .config("spark.sql.catalogImplementation", "hive") \
#         .config("spark.sql.warehouse.dir", "file:///Users/jinyuanzhang/spark-warehouse") \
#         .getOrCreate()
#
#     hadoop_conf = spark._jsc.hadoopConfiguration()
#     hadoop_conf.set("fs.s3a.access.key", "admin")
#     hadoop_conf.set("fs.s3a.secret.key", "admin123")
#     hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
#     hadoop_conf.set("fs.s3a.path.style.access", "true")
#     hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
#     hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
#
#     return spark
#
# # 3. Bronze Layer: 读取原始 Parquet
#
# def load_bronze(spark):
#     return spark.read.parquet("s3a://pedigrees360-datalake/horse-delta/horses.parquet")
#
# # 4. Silver Layer: 数据清洗标准化
#
# def transform_silver(df):
#     cleaned_df = df.withColumn("name", trim(upper(col("name")))) \
#                  .withColumn("gender", trim(upper(col("gender")))) \
#                  .withColumn("sire_name", trim(upper(col("sire_name")))) \
#                  .withColumn("dam_name", trim(upper(col("dam_name"))))
#
#     cleaned_df = cleaned_df.filter((col("birth_year") >= 1900) & (col("birth_year") <= 2025))
#
#     return cleaned_df
#
# # 5. Gold Layer: 特征工程
#
# def transform_gold(df):
#     enriched_df = df.withColumn("age", 2025 - col("birth_year"))
#     return enriched_df
#
# # 6. 保存 Delta表
#
# def save_delta(df, path):
#     df.write.format("delta") \
#       .mode("overwrite") \
#       .save(path)
#
# # 7. 注册到 Hive Metastore
#
# def register_table(spark, table_name, delta_path):
#     spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {table_name}
#     USING DELTA
#     LOCATION '{delta_path}'
#     """)
#
# # 8. 主流程
#
# def main():
#     export_postgres_to_s3()
#
#     spark = init_spark()
#
#     bronze_df = load_bronze(spark)
#     silver_df = transform_silver(bronze_df)
#     gold_df = transform_gold(silver_df)
#
#     save_delta(silver_df, "s3a://pedigrees360-datalake/horse-delta/silver-horses")
#     save_delta(gold_df, "s3a://pedigrees360-datalake/horse-delta/gold-horses")
#
#     register_table(spark, "horse_silver_table", "s3a://pedigrees360-datalake/horse-delta/silver-horses")
#     register_table(spark, "horse_gold_table", "s3a://pedigrees360-datalake/horse-delta/gold-horses")
#
#     print("\n✅ ETL Process Completed and Tables Registered in Hive Metastore!\n")
#
# if __name__ == "__main__":
#     main()



import os
import psycopg2
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

def export_postgres_to_s3():
    conn = psycopg2.connect(
        host="localhost",
        port=5434,
        user="admin",
        password="admin123",
        dbname="horses"
    )
    cursor = conn.cursor()
    cursor.execute('SELECT id, name, gender, birth_year, sire_name, dam_name, is_champion, inbreeding_score, defect_risk_score FROM horses')
    rows = cursor.fetchall()

    schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('gender', pa.string()),
        ('birth_year', pa.int32()),
        ('sire_name', pa.string()),
        ('dam_name', pa.string()),
        ('is_champion', pa.bool_()),
        ('inbreeding_score', pa.float32()),
        ('defect_risk_score', pa.float32()),
    ])

    table = pa.Table.from_pylist([{
        "id": r[0],
        "name": r[1],
        "gender": r[2],
        "birth_year": r[3],
        "sire_name": r[4],
        "dam_name": r[5],
        "is_champion": r[6],
        "inbreeding_score": r[7],
        "defect_risk_score": r[8]
    } for r in rows], schema=schema)

    pq.write_table(table, 'horses.parquet')
    print("\u2705 Parquet file created locally.")

    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='admin123',
        region_name='us-east-1'
    )

    with open('horses.parquet', 'rb') as f:
        s3.upload_fileobj(f, 'pedigrees360-datalake', 'horse-delta/horses.parquet')

    print("\u2705 Parquet file uploaded to MinIO S3.")

    cursor.close()
    conn.close()

def init_spark():
    spark = SparkSession.builder \
        .appName("Pedigrees360 ETL Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "file:///Users/jinyuanzhang/spark-warehouse") \
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

def load_bronze(spark):
    return spark.read.parquet("s3a://pedigrees360-datalake/horse-delta/horses.parquet")

def transform_silver(df):
    cleaned_df = df.withColumn("name", trim(upper(col("name")))) \
        .withColumn("gender", trim(upper(col("gender")))) \
        .withColumn("sire_name", trim(upper(col("sire_name")))) \
        .withColumn("dam_name", trim(upper(col("dam_name"))))

    cleaned_df = cleaned_df.filter((col("birth_year") >= 1900) & (col("birth_year") <= 2025))

    return cleaned_df

def transform_gold(df):
    enriched_df = df.withColumn("age", 2025 - col("birth_year"))
    return enriched_df

def save_delta(df, path):
    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save(path)

def register_table(spark, table_name, delta_path):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    USING DELTA
    LOCATION '{delta_path}'
    """)

def main():
    export_postgres_to_s3()
    spark = init_spark()

    bronze_df = load_bronze(spark)
    silver_df = transform_silver(bronze_df)
    gold_df = transform_gold(silver_df)

    save_delta(silver_df, "s3a://pedigrees360-datalake/horse-delta/silver-horses")
    save_delta(gold_df, "s3a://pedigrees360-datalake/horse-delta/gold-horses")

    register_table(spark, "horse_silver_table", "s3a://pedigrees360-datalake/horse-delta/silver-horses")
    register_table(spark, "horse_gold_table", "s3a://pedigrees360-datalake/horse-delta/gold-horses")

    print("\n\u2705 ETL Process Completed and Tables Registered in Hive Metastore!\n")

if __name__ == "__main__":
    main()