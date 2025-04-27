import pandas as pd
import lightgbm as lgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import pickle
from lightgbm import callback

# 1. 读取数据
X_train = pd.read_csv("X_train.csv")
y_train = pd.read_csv("y_train.csv")
X_val = pd.read_csv("X_val.csv")
y_val = pd.read_csv("y_val.csv")

# 2. Quick Check Training Data
print("🔍 开始检查训练数据基本信息...")
print(f"X_train shape: {X_train.shape}")
print(f"y_train shape: {y_train.shape}")

# 样本总数
print(f"训练样本总数: {len(y_train)}")

# 正负样本比例
positive = (y_train.values.ravel() == 1).sum()
negative = (y_train.values.ravel() == 0).sum()
print(f"正样本数 (label=1): {positive}")
print(f"负样本数 (label=0): {negative}")
print(f"正负样本比例: {positive / (negative + 1e-9):.4f}")

# 检查特征是否异常
print("\n🔍 开始检查特征异常...")
zero_variance_features = (X_train.std() == 0).sum()
print(f"标准差为0的特征数量（无用特征）: {zero_variance_features}")

missing_values_features = (X_train.isnull().sum() > 0).sum()
print(f"含有缺失值的特征数量: {missing_values_features}")

print("✅ 训练数据检查完毕。\n")

# 3. 转换为 LightGBM 数据格式
train_data = lgb.Dataset(X_train, label=y_train.values.ravel())
val_data = lgb.Dataset(X_val, label=y_val.values.ravel(), reference=train_data)

# 4. 设置 LightGBM 超参数
params = {
    'objective': 'binary',
    'metric': 'binary_logloss',
    'boosting_type': 'gbdt',
    'num_leaves': 31,
    'learning_rate': 0.1,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'scale_pos_weight': 2,
}

# 5. 定义回调函数
early_stopping_callback = callback.early_stopping(stopping_rounds=30)
log_callback = callback.log_evaluation(period=10)
evals_result = {}
record_eval_callback = callback.record_evaluation(evals_result)

# 6. 训练模型
print("✅ 开始训练 LightGBM 模型...")

model = lgb.train(
    params,
    train_data,
    valid_sets=[train_data, val_data],
    valid_names=['train', 'valid'],
    num_boost_round=2000,
    callbacks=[early_stopping_callback, log_callback, record_eval_callback]
)

# 7. 获取最佳迭代次数
best_iteration = model.best_iteration
print(f"✅ 最佳迭代次数: {best_iteration}")

# 8. 验证集预测
y_pred = model.predict(X_val, num_iteration=best_iteration)
y_pred_bin = (y_pred > 0.5).astype(int)

# 9. 输出评估指标
acc = accuracy_score(y_val, y_pred_bin)
precision = precision_score(y_val, y_pred_bin, zero_division=0)
recall = recall_score(y_val, y_pred_bin, zero_division=0)
f1 = f1_score(y_val, y_pred_bin, zero_division=0)

print(f"✅ 验证集评估结果：")
print(f"Accuracy (准确率): {acc:.4f}")
print(f"Precision (精确率): {precision:.4f}")
print(f"Recall (召回率): {recall:.4f}")
print(f"F1 Score: {f1:.4f}")

# 10. 保存模型
with open("lightgbm_model.pkl", "wb") as f:
    pickle.dump(model, f)

print("✅ 模型训练完成并已保存：lightgbm_model.pkl")