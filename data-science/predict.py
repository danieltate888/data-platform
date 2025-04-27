
import warnings
import pandas as pd
import pickle
import sys
import json
from sklearn.preprocessing import LabelEncoder

warnings.filterwarnings('ignore')

# 从命令行获取传入的特征
features = json.loads(sys.argv[1])  # 将传入的字符串转换为列表

# 定义列名（和你传进来的数据一一对应）
column_names = [
    'id', 'gender', 'sire_name', 'dam_name', 'birth_year',
    'inbreeding_score', 'defect_risk_score', 'pagerank',
    'degree', 'out_degree'
]

# 创建DataFrame
X_new = pd.DataFrame([features], columns=column_names)

# 加载模型和标准化器
with open('logistic_model.pkl', 'rb') as f:
    model = pickle.load(f)

with open('scaler.pkl', 'rb') as f:
    scaler = pickle.load(f)

# 按训练时特征顺序重新排列（注意id开头）
expected_columns = [
    'id', 'gender', 'sire_name', 'dam_name', 'birth_year',
    'inbreeding_score', 'defect_risk_score', 'pagerank',
    'degree', 'out_degree'
]
X_new = X_new[expected_columns]

# 对类别特征进行Label Encoding
categorical_cols = ['gender', 'sire_name', 'dam_name']
for col in categorical_cols:
    if col in X_new.columns:
        le = LabelEncoder()
        X_new[col] = le.fit_transform(X_new[col].astype(str))

# 标准化之前，必须去掉列名，转成 numpy array
X_new = X_new.values

# 标准化
X_new_scaled = scaler.transform(X_new)

# 做出预测
prediction = model.predict(X_new_scaled)

# 输出预测结果
print(prediction[0])