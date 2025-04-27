# import psycopg2
# import pandas as pd
#
# def fetch_postgres_features():
#     conn = psycopg2.connect(
#         host="localhost",
#         port=5434,
#         user="admin",
#         password="admin123",
#         dbname="horses"
#     )
#     query = """
#             SELECT id, name, gender, birth_year, sire_name, dam_name, inbreeding_score, defect_risk_score, is_champion
#             FROM horses \
#             """
#     df = pd.read_sql_query(query, conn)
#     conn.close()
#     return df
#
# postgres_features = fetch_postgres_features()
# print(postgres_features.head())
#
# # 保存到CSV
# output_file = "postgres_horse_features.csv"
# postgres_features.to_csv(output_file, index=False)
# print(f"✅ Feature table has been successfully saved to '{output_file}'.")

import psycopg2
import pandas as pd

def fetch_postgres_features():
    conn = psycopg2.connect(
        host="localhost",
        port=5434,
        user="admin",
        password="admin123",
        dbname="horses"
    )
    query = """
            SELECT id, name, gender, birth_year, sire_name, dam_name, inbreeding_score, defect_risk_score, is_champion
            FROM horses \
            """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

if __name__ == "__main__":
    postgres_features = fetch_postgres_features()
    postgres_features.to_csv("postgres_horse_features.csv", index=False)
    print("✅ Postgres features saved.")