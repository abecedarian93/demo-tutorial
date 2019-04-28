package com.abecedarian.demo.spark

import org.apache.hadoop.io.NullWritable
import org.apache.orc.mapred.{OrcInputFormat, OrcStruct}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abecedarian on 2019/4/26
  *
  */
object FileIODemo extends App {

  override def main(args: Array[String]): Unit = {

    print("301e288b-5982-4f85-9916-6a86dac3402d".length)

    val input = args(0)
    val conf = new SparkConf()
    conf.setAppName("FileIODemo_abecedarian")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    sc.textFile(input)
      .map(x=>{
        val sb=new StringBuilder
        val items=x.split(",")
        val day=items(0)
        val t1=items(1)
        val t2=items(2)
        val t3=items(3)
        val days=day.split("-|/")
        if(days.length>2){
          sb.append(days(0)).append("-")
          sb.append(days(1)).append("-")
          sb.append(days(2))
        }
        sb.toString()+"\t"+t1+"\t"+t2+"\t"+t3
      })
      .foreach(println)


  }

}
