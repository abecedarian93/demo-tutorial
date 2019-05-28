# coding:utf-8

from __future__ import print_function

import tensorflow as tf

# 创建两个常量op a,b
a = tf.constant(2)
b = tf.constant(3)

# 用默认graph启动
with tf.Session() as sess:
    print("a=2,b=3")
    print("a+b: %i" % sess.run(a + b))
    print("a*b: %i" % sess.run(a + b))

# 创建两个int16类型的op a,b
a = tf.placeholder(tf.int16)
b = tf.placeholder(tf.int16)

# 定义一些操作
add=tf.add(a,b)
mul=tf.multiply(a,b)

# 用默认graph启动
with tf.Session() as sess:
    print("a+b: %i" % sess.run(add, feed_dict={a: 2, b: 3}))
    print("a*b: %i" % sess.run(mul, feed_dict={a: 2, b: 3}))


# 定义两个矩阵常量 matrix1,matrix2
matrix1=tf.constant([[3.,3.]])
matrix2=tf.constant([[2.],[2.]])

product=tf.matmul(matrix1,matrix2)

with tf.Session() as sess:
    result=sess.run(product)
    print(result)
