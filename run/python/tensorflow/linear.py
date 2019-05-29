# coding:utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import pandas as pd
import tensorflow as tf

from sklearn.model_selection import train_test_split

tf.logging.set_verbosity(tf.logging.INFO)

# 工程目录
project_path = "/Users/abecedarian/Workspace/demo-tutorial/"

# 读取csv文件
df = pd.read_csv(project_path + 'data/tensorflow/level.csv')

# 对读取的dataFrame进行操作
df['city_geo'] = df.city_geo.fillna("null").astype(str)
df['gender'] = df.gender.replace('\\N', '-1')
df['age'] = df.age.replace('\\N', '0').astype(int)

# 提取标签
y = df.pop('classes')

# 评估数据为25%,训练数据为75%
X_train, X_test, y_train, y_test = train_test_split(df, y, test_size=0.25, random_state=19)


# 输入函数的构造函数
def make_input_fn(X, y, shuffle=True, batch_size=2, shuffle_length=10000):
    def input_fn():
        ds = tf.data.Dataset.from_tensor_slices((X.to_dict(orient='list'), y))
        # 乱序
        if shuffle:
            ds = ds.shuffle(shuffle_length)

        ds = ds.batch(batch_size)

        return ds

    return input_fn


# 训练、评估所使用的数据输入函数
train_input_fn = make_input_fn(X_train, y_train, shuffle=True)
eval_input_fn = make_input_fn(X_test, y_test, shuffle=False)


# 辅助函数,把给定数据列做one-hot编码
def one_hot_cat_column(feature_name, vocab):
    return tf.feature_column.indicator_column(
        tf.feature_column.categorical_column_with_vocabulary_list(feature_name, vocab))


# 最终使用的数据列
model_fn = []

# 城市区域one-hot编码
city_geo_voc = df['city_geo'].unique()
city_geo_categorical = one_hot_cat_column('city_geo', city_geo_voc)
model_fn.append(city_geo_categorical)

# 性别one-hot编码
gender_voc = df['gender'].unique()
gender_categorical = one_hot_cat_column('gender', gender_voc)
model_fn.append(gender_categorical)

# 年龄分段
age_boundaries = [18, 25, 30, 35, 40, 45, 50, 55, 60, 65]
age = tf.feature_column.numeric_column('age')
age_buckets = tf.feature_column.bucketized_column(
    age, boundaries=age_boundaries)
model_fn.append(age_buckets)

# 定义线性回归器
est = tf.estimator.LinearRegressor(
    feature_columns=model_fn,  # 自定义的特征列
    model_dir="/tmp/level",
    optimizer=tf.train.FtrlOptimizer(  # 选择合适的优化函数
        learning_rate=0.1,
        l1_regularization_strength=0.001
    )
)

# 开始训练
est.train(input_fn=train_input_fn, max_steps=5000)  # 输入函数喂数据
# 测试集评估
metrics = est.evaluate(input_fn=eval_input_fn)

# 输出评估效果
print(pd.Series(metrics))

print(est.get_variable_names())

# 整合每个特征对应的权重和bias的值,方便人工分析其模型效果
age_bucketized_weights = est.get_variable_value('linear/linear_model/age_bucketized/weights').flatten()
city_geo_indicator_weights = est.get_variable_value('linear/linear_model/city_geo_indicator/weights').flatten()
gender_indicator_weights = est.get_variable_value('linear/linear_model/gender_indicator/weights').flatten()
bias = est.get_variable_value('linear/linear_model/bias_weights').flatten()

age_boundaries.append('max')
age_feature_weights = dict(zip(["age#" + str(x) for x in age_boundaries], age_bucketized_weights.tolist()))
city_geo_feature_weights = dict(zip(["city_geo#" + str(x) for x in city_geo_voc], city_geo_indicator_weights.tolist()))
gender_feature_weights = dict(zip(["gender#" + str(x) for x in gender_voc], gender_indicator_weights.tolist()))

bias_dict = {"bias": str(bias)}
feature_weights = {**age_feature_weights, **city_geo_feature_weights, **gender_feature_weights, **bias_dict}

# 整合每个特征对应的频次,方便人工分析其模型效果//todo
city_geo_frequency=df.groupby('city_geo')['age'].count().to_dict()
gender_frequency=df.groupby('gender')['city_geo'].count().to_dict()


print(feature_weights)
