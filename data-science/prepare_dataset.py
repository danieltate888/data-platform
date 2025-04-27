# import pandas as pd
# from sklearn.model_selection import train_test_split
# from sklearn.preprocessing import LabelEncoder
#
# # 1. 读取数据
# X = pd.read_csv("X_features.csv")
# y = pd.read_csv("y_labels.csv")
#
# # 💥💥💥 加这句，删除name列
# if 'name' in X.columns:
#     X = X.drop(columns=['name'])
#
# # 2. 类别特征编码
# categorical_cols = ['gender', 'sire_name', 'dam_name']
# encoders = {}
# for col in categorical_cols:
#     le = LabelEncoder()
#     X[col] = le.fit_transform(X[col].astype(str))
#     encoders[col] = le
#
# # 3. 切分训练集和验证集
# X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
#
# # 4. 保存
# X_train.to_csv("X_train.csv", index=False)
# X_val.to_csv("X_val.csv", index=False)
# y_train.to_csv("y_train.csv", index=False)
# y_val.to_csv("y_val.csv", index=False)
#
# print("✅ 数据集准备完成：X_train.csv, X_val.csv, y_train.csv, y_val.csv 已保存。")

import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

# 读取特征文件
postgres_features = pd.read_csv("postgres_horse_features.csv")
neo4j_features = pd.read_csv("neo4j_horse_features.csv")

# 合并特征
full_features = pd.merge(postgres_features, neo4j_features, on="id")

# 处理类别特征
for col in ["gender", "sire_name", "dam_name"]:
    if col in full_features.columns:
        le = LabelEncoder()
        full_features[col] = le.fit_transform(full_features[col].astype(str))

# 拆分特征和标签
X = full_features.drop(columns=["is_champion", "name"])  # name 删除，不用于训练
y = full_features["is_champion"]

# 填充缺失值
X = X.fillna(0)

# 切分训练验证集
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# 删除标准差为0的特征
valid_columns = X_train.columns[X_train.std() != 0]
X_train = X_train[valid_columns]
X_val = X_val[valid_columns]

# 保存到文件
X_train.to_csv("X_train.csv", index=False)
X_val.to_csv("X_val.csv", index=False)
y_train.to_csv("y_train.csv", index=False)
y_val.to_csv("y_val.csv", index=False)

print("✅ Dataset preparation complete.")