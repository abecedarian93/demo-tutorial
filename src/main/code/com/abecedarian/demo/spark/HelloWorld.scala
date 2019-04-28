package com.abecedarian.demo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2019/3/28
  *
  */
object HelloWorld extends App {

  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkHelloWorld")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/spark/helloworld.txt")
    val wc = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wc.foreach(println)


    Thread.sleep(10 * 60 * 1000) // 挂住 10 分钟; 这时可以去看 SparkUI: http://localhost:4040
  }

}
