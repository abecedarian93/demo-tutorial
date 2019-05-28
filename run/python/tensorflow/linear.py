# coding:utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import pandas as pd
import tensorflow as tf

from sklearn.model_selection import train_test_split

tf.logging.set_verbosity(tf.logging.INFO)

#工程目录
project_path="/Users/abecedarian/Workspace/demo-tutorial/"

#读取csv文件
df = pd.read_csv(project_path+'data/tensorflow/level.csv')

#对读取的dataFrame进行操作
df['city_geo'] = df.city_geo.fillna("null").astype(str)
df['gender'] = df.gender.replace('\\N', '-1')
df['age'] = df.age.replace('\\N', '0').astype(int)

#提取标签
y = df.pop('classes')

#评估数据为25%,训练数据为75%
X_train, X_test, y_train, y_test = train_test_split(df, y, test_size=0.25, random_state=20)

#输入函数的构造函数
def make_input_fn(X, y, shuffle=True, batch_size=0,shuffle_length=10000):
    def input_fn():
        ds = tf.data.Dataset.from_tensor_slices((X.to_dict(orient='list'), y))
        #乱序
        if shuffle:
            ds = ds.shuffle(shuffle_length)

        ds = ds.batch(batch_size)

        return ds

    return input_fn

# 训练、评估所使用的数据输入函数
train_input_fn = make_input_fn(X_train, y_train, shuffle=True)
eval_input_fn = make_input_fn(X_test, y_test, shuffle=False)

#辅助函数,把给定数据列做one-hot编码
def one_hot_cat_column(feature_name, vocab):
    return tf.feature_column.indicator_column(
        tf.feature_column.categorical_column_with_vocabulary_list(feature_name, vocab))

#定义所需的数据列
#分类型属性
CATEGORICAL_COLUMNS = ['city_geo', 'gender']
#数值型属性
NUMERIC_COLUMNS = ['age']

#最终使用的数据列
model_fn = []
for feature_name in CATEGORICAL_COLUMNS:
    #分类的属性都要做one-hot编码,然后加入数据列
    vocabulary = df[feature_name].unique()
    model_fn.append(one_hot_cat_column(feature_name, vocabulary))

for feature_name in NUMERIC_COLUMNS:
    #数值类的属性直接入列
    model_fn.append(tf.feature_column.numeric_column(feature_name, dtype=tf.float32))

# 定义线性回归器
classifier = tf.estimator.LinearRegressor(
    feature_columns=model_fn, # 自定义的特征列
    model_dir="/tmp/level",
    optimizer=tf.train.FtrlOptimizer( #选择合适的优化函数
        learning_rate=0.1,
        l1_regularization_strength=0.001
    )
)

#开始训练
classifier.train(input_fn=train_input_fn, max_steps=500) #输入函数喂取数据
#测试集评估
metrics = classifier.evaluate(input_fn=eval_input_fn)

#输出评估效果
print(pd.Series(metrics))
