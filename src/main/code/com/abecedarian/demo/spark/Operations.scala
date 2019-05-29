package com.abecedarian.demo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2019/2/25
  *
  */
object Operations extends App {

  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Operations_abecedarian")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(Seq("abecedarian-01", "abecedarian-01", "abecedarian-02", "abecedarian-03", "abecedarian-04", "abecedarian-05", "abecedarian-06", "abecedarian-07", "abecedarian-08"), 3)
    val pairRdd = sc.parallelize(Seq(("abecedarian", "01"), ("abecedarian", "02"), ("abc", "01"), ("abc", "02"), ("abc", "03"), ("abcd", "01")))
    val pair2Rdd = sc.parallelize(Seq(("abecedarian", "01-2"), ("abecedarian", "02-2"), ("abc", "01-2"), ("abc", "02-2"), ("abcf", "03-2"), ("abcf", "01-2")))

    //map算子
    val mapRdd = rdd.map(line => line + "-map")
    //    mapRdd.foreach(println)
    //flatMap算子
    val flatMapRdd = rdd.flatMap(line => line.split("-"))
    //    flatMapRdd.foreach(println)
    //mapPartitions算子
    val mapPartitionsRdd = rdd.mapPartitions(line => {
      val result = List[String]()
      val sb = new StringBuilder
      while (line.hasNext) {
        sb.append(line.next())
      }
      result.::(sb).iterator
    })
    //    mapPartitionsRdd.foreach(println)
    //map的输入变换函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区

    //glom算子
    val glomRdd = rdd.glom()
    glomRdd.collect().foreach(line => {
      line.foreach(print)
      println
    })

    //union算子 两个RDD进行合并，不去重
    val unionRdd = mapRdd.union(rdd)
    unionRdd.foreach(println)

    //intersection算子 返回两个RDD的交集，并且去重
    val intersectionRdd = mapRdd.intersection(rdd)
    intersectionRdd.foreach(println)

    //cartesian算子 所有元素进行笛卡尔积操作
    val cartesianRdd = mapRdd.cartesian(rdd)
    cartesianRdd.foreach(println)

    //filter算子
    val filterMap = rdd.filter(line => line.contains("02"))
    filterMap.foreach(println)

    //groupBy算子
    val groupByRdd = pairRdd.groupBy(line => line._1)
    groupByRdd.foreach(println)

    //groupByKey算子
    val groupByKeyRdd = pairRdd.groupByKey().map(line => {
      val sb = new StringBuilder
      line._2.foreach(sb.append(_).append("#"))
      (line._1, sb)
    })
    groupByKeyRdd.foreach(println)

    //reduceByKey算子
    val reduceByKeyRdd = pairRdd.reduceByKey((x1, x2) => x1 + "#" + x2)
    reduceByKeyRdd.foreach(println)

    //distinct算子
    val distinctRdd = rdd.distinct()
    distinctRdd.foreach(println)

    //join算子
    val joinRdd = pairRdd.join(pair2Rdd)
    joinRdd.foreach(println)

    println("leftOuterJoin算子")
    //leftOuterJoin算子
    val leftOuterJoinRdd = pairRdd.leftOuterJoin(pair2Rdd)
    leftOuterJoinRdd.foreach(println)

    println("rightOuterJoin算子")
    //rightOuterJoin算子
    val rightOuterJoinRdd = pairRdd.rightOuterJoin(pair2Rdd)
    rightOuterJoinRdd.foreach(println)
  }


}
