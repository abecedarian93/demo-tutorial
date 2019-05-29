package com.abecedarian.demo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2018/12/3
  * Spark scala Accumulator(累加器) 入门demo
  *
  */
object Accumulator extends App {

  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Accumulator_abecedarian")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sparkAcc = sc.longAccumulator

    val lines = sc.textFile("data/spark/helloworld.txt")
    lines.foreach(line => {
      val its = line.split(" ")
      for (item <- its) {
        if ("spark".equalsIgnoreCase(item)) sparkAcc.add(1)
      }
    })

    println("spark word count: " + sparkAcc.value)

    sc.stop()
  }

}
