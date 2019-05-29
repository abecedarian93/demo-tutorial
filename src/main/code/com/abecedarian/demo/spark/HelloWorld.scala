package com.abecedarian.demo.spark

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2019/3/28
  *
  */
object HelloWorld extends App {

  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("HelloWorld_abecedarian")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/spark/helloworld.txt")

    val wc = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((v1, v2) => v1 + v2)
    //简写方式-不熟悉scala不建议使用
    val wc2 = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    //wc结果打印
    wc.foreach(println)

    //wc结果保持到/tmp/save_wc,且文件个数为2
    wc.coalesce(numPartitions = 2, shuffle = false).saveAsTextFile("/tmp/save_wc", classOf[GzipCodec])

    Thread.sleep(10 * 60 * 1000) // 挂住 10 分钟; 这时可以去看 SparkUI: http://localhost:4040
  }

}
