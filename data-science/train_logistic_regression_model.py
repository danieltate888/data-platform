import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import pickle

# 读数据
X_train = pd.read_csv("X_train.csv")
y_train = pd.read_csv("y_train.csv")
X_val = pd.read_csv("X_val.csv")
y_val = pd.read_csv("y_val.csv")

# 特征标准化
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_val = scaler.transform(X_val)

# 保存Scaler
with open("scaler.pkl", "wb") as f:
    pickle.dump(scaler, f)

# 训练模型
print("✅ 开始训练 Logistic Regression 模型...")
model = LogisticRegression(
    penalty='l2',
    C=1.0,
    solver='liblinear',
    class_weight='balanced',
    max_iter=1000
)
model.fit(X_train, y_train.values.ravel())
print("✅ 模型训练完成。")

# 验证集评估
y_pred = model.predict(X_val)
acc = accuracy_score(y_val, y_pred)
precision = precision_score(y_val, y_pred, zero_division=0)
recall = recall_score(y_val, y_pred, zero_division=0)
f1 = f1_score(y_val, y_pred, zero_division=0)

print("✅ 验证集评估结果：")
print(f"Accuracy (准确率): {acc:.4f}")
print(f"Precision (精确率): {precision:.4f}")
print(f"Recall (召回率): {recall:.4f}")
print(f"F1 Score: {f1:.4f}")

# 保存模型
with open("logistic_model.pkl", "wb") as f:
    pickle.dump(model, f)

print("✅ Logistic Regression 模型已保存。")
