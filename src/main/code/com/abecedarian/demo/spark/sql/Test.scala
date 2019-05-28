package com.abecedarian.demo.spark.sql

import java.sql.Struct

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

import scala.collection.mutable
import org.apache.spark.sql.functions._


/**
  * Created by abecedarian on 2019/5/10
  *
  */
object Test extends App {


  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val df = spark.read.json("/Users/abecedarian/Documents/Data/Spark/input/json.txt")

    df.createOrReplaceTempView("tmp_json")


    val result = spark.sql(
      "select gid" +
        " from tmp_json" +
        " where array_contains(show.channelId,'ucnew_rtb')=true" +
        " having size(show)>30 and size(click)<2 and size(deeplink)<2")


    result
      .write
      .mode(SaveMode.Overwrite)
      .format("text")
      .save("/Users/abecedarian/Documents/Data/Spark/output/sql")
    //      .save("/user/wirelessdev/tmp/tianle.li/spark/ucnew_rtb")


  }

}
