package com.abecedarian.demo.scala

/**
  * Created by abecedarian on 2019/4/30
  *
  */
class SingletonDemo(val v: String) {

  println("创建:" + this)

  override def toString(): String = "singleton类变量:" + v
}

object SingletonDemo {

  def apply(v: String) = {

    new SingletonDemo(v)
  }

  def main(args: Array[String]) = {
    println(SingletonDemo("hello scala singleton"))
  }

}

