package com.abecedarian.demo.spark

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2019/2/22
  * Spark scala wordCount 入门demo
  *
  * 在公司yarn集群运行该程序脚本:
  * sudo -uwirelessdev spark-submit \
  * --class com.abecedarian.demo.spark.WordCountDemo \
  * --master yarn \
  * --deploy-mode cluster \
  * --driver-memory 4g \
  * --executor-memory 12g \
  * --num-executors 20 \
  * --executor-cores 2 \
  * --queue wirelessdev \
  * --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=6144m -XX:PermSize=1024m" \
  * --conf spark.app.name=WordCountDemo_abecedarian \
  * ${jarPath}/market_statistics-jar-with-dependencies.jar \
  * ${input} ${output} ${numPartitions}
  *
  *@jarPath jar路径 （/home/q/work/market_statistics/market_statistics/market_statistics_offline）注意：先执行build.sh打包
  *@input 文件输入路径 (/user/wirelessdev/result/demo/mr/input/wordcount.txt)
  *@output 文件输出路径 (/user/wirelessdev/result/demo/spark/output/wordcount)
  *@numPartitions 文件输出分区个数 (2)
  *
  */
object WordCountDemo extends App {

  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    /** 本地debug开启 **/
//    conf.setAppName("WordCountDemo_abecedarian")
//    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))
    val output = args(1)
    val wordCount = input.flatMap(line => line.split(" ")).filter(line => StringUtils.isNotBlank(line)).map(line => (line, 1)).reduceByKey((o1, o2) => o1 + o2)
    /** 简写方式-不熟悉spark不建议使用**/
//    val wordCount2=input.flatMap(_.split(" ")).filter(StringUtils.isNotBlank).map((_,1)).reduceByKey(_+_)
    /** coalesce 指定输出文件个数 （如分区数量未达到指定文件个数，以分区数量为准） **/
    /** classOf[GzipCodec] 指定输出文件压缩格式，其他常用压缩格式还有classOf[Lz4Codec]，classOf[SnappyCodec] **/
    wordCount.coalesce(args(2).toInt).saveAsTextFile(output, classOf[GzipCodec])

    sc.stop()
  }

}
