import pandas as pd

# 1. 读取两个CSV文件
postgres_features = pd.read_csv("postgres_horse_features.csv")
neo4j_features = pd.read_csv("neo4j_horse_features.csv")

print("✅ 成功读取输入文件。")
print("postgres_features shape:", postgres_features.shape)
print("neo4j_features shape:", neo4j_features.shape)

# 2. 按id进行merge
full_features = pd.merge(postgres_features, neo4j_features, on="id")

print("✅ 成功合并数据。")
print("full_features shape:", full_features.shape)

# 3. 把 is_champion 单独分离出来作为标签 y
X = full_features.drop(columns=["is_champion"])
y = full_features["is_champion"]

# 4. 保存成新的CSV文件
X.to_csv("X_features.csv", index=False)
y.to_csv("y_labels.csv", index=False)

print("✅ 特征 (X_features.csv) 和标签 (y_labels.csv) 保存完成。")