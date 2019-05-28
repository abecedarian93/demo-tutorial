# coding:utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import pandas as pd
import tensorflow as tf

from sklearn.model_selection import train_test_split

#工程目录
project_path="/Users/abecedarian/Workspace/demo-tutorial/"

tf.logging.set_verbosity(tf.logging.INFO)

df = pd.read_csv(project_path+'data/tensorflow/level.csv')

df['city_geo'] = df.city_geo.fillna("null").astype(str)
df['gender'] = df.gender.replace('\\N', '-1')
df['age'] = df.age.replace('\\N', '0').astype(int)

y = df.pop('classes')

X_train, X_test, y_train, y_test = train_test_split(df, y, test_size=0.25, random_state=20)

def make_input_fn(X, y, shuffle=True, batch_size=0,shuffle_length=10000):
    def input_fn():
        ds = tf.data.Dataset.from_tensor_slices((X.to_dict(orient='list'), y))
        if shuffle:
            ds = ds.shuffle(shuffle_length)

        ds = ds.batch(batch_size)

        return ds

    return input_fn

train_input_fn = make_input_fn(X_train, y_train, shuffle=True, batch_size=1)
eval_input_fn = make_input_fn(X_test, y_test, shuffle=True, batch_size=1)

def one_hot_cat_column(feature_name, vocab):
    return tf.feature_column.indicator_column(
        tf.feature_column.categorical_column_with_vocabulary_list(feature_name, vocab))


CATEGORICAL_COLUMNS = ['city_geo', 'gender']
NUMERIC_COLUMNS = ['age']

model_fn = []
for feature_name in CATEGORICAL_COLUMNS:
    vocabulary = df[feature_name].unique()
    model_fn.append(one_hot_cat_column(feature_name, vocabulary))

for feature_name in NUMERIC_COLUMNS:
    model_fn.append(tf.feature_column.numeric_column(feature_name, dtype=tf.float32))


classifier = tf.estimator.LinearRegressor(
    feature_columns=model_fn,
    model_dir="/tmp/level",
    optimizer=tf.train.FtrlOptimizer(
        learning_rate=0.1,
        l1_regularization_strength=0.001
    )
)

classifier.train(input_fn=train_input_fn, max_steps=500)
metrics = classifier.evaluate(input_fn=eval_input_fn)

print(pd.Series(metrics))
