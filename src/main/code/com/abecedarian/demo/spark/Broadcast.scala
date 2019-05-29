package com.abecedarian.demo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2019/5/29
  *
  * Spark Broadcast(广播变量) 入门demo
  */
object Broadcast extends App {

  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Broadcast_abecedarian")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/spark/helloworld.txt")

    //广播变量分隔符,供executor使用
    //广播变量指将driver端的数据分发到executor供其使用,分发方式类似BT
    val separator = sc.broadcast(" ")

    val wc = lines.flatMap(_.split(separator.value)).map(word => (word, 1)).reduceByKey(_ + _)

    wc.foreach(println)
  }

}
