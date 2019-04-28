package com.abecedarian.demo.spark


import org.apache.hadoop.io.NullWritable
import org.apache.orc.mapred.{OrcInputFormat, OrcStruct}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2019/4/24
  *
  * spark 读取orc格式文件
  */
object OrcIODemo extends App {

  override def main(args: Array[String]): Unit = {

    val conf=new SparkConf()
    conf.setAppName("orcIODemo_abecedarian")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val orcFile=sc.hadoopFile[NullWritable, OrcStruct, OrcInputFormat[OrcStruct]]("data/spark/sample.orc")

    orcFile.map(x=>{
      x._2.toString
    }).foreach(println)


  }

}
