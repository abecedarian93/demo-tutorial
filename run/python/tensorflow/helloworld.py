# coding:utf-8

from __future__ import print_function

import tensorflow as tf

# 创建一个常量op
hello = tf.constant('Hello, TensorFlow!')

#启动tf sess
sess = tf.Session()

#运行op
print(sess.run(hello))