import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import pickle

# 1. 读训练数据
X_train = pd.read_csv("X_train.csv")
y_train = pd.read_csv("y_train.csv")

# 2. 初始化模型
model = RandomForestClassifier(n_estimators=100, random_state=42)

# 3. 训练模型
model.fit(X_train, y_train.values.ravel())

# 4. 保存模型
with open("random_forest_model.pkl", "wb") as f:
    pickle.dump(model, f)

print("✅ 模型训练完成并已保存：random_forest_model.pkl")