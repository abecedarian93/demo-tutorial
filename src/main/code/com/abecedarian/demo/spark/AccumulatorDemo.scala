package com.abecedarian.demo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2018/12/3
  * Spark scala Accumulator(累加器) 入门demo
  *
  * 在公司yarn集群运行该程序脚本:
  * sudo -uwirelessdev spark-submit \
  * --class com.abecedarian.demo.spark.AccumulatorDemo \
  * --master yarn \
  * --deploy-mode cluster \
  * --driver-memory 4g \
  * --executor-memory 12g \
  * --num-executors 20 \
  * --executor-cores 2 \
  * --queue wirelessdev \
  * --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=6144m -XX:PermSize=1024m" \
  * --conf spark.app.name=AccumulatorDemo_abecedarian \
  * ${jarPath}/market_statistics-jar-with-dependencies.jar \
  * ${input}
  *
  * @jarPath jar路径
  * @input 文件输入路径 (/user/wirelessdev/result/demo/mr/input/input1.txt)
  *
  */
object AccumulatorDemo extends App {


  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    /** 本地debug开启 **/
//    conf.setAppName("AccumulatorDemo_abecedarian")
//    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val adrAccumulator = sc.longAccumulator
    val iosAccumulator = sc.longAccumulator
    val totalAccumulator = sc.longAccumulator
    val input = sc.textFile(args(0))
    input.foreach(line => {
      val iteams = line.split(",")
      if ("adr".equals(iteams(1))) adrAccumulator.add(1)
      else if ("ios".equals(iteams(1))) iosAccumulator.add(1)
      totalAccumulator.add(1)
    })

    println("adr: " + adrAccumulator.value)
    println("ios: " + iosAccumulator.value)
    println("total: " + totalAccumulator.value)
    sc.stop()
  }

}
