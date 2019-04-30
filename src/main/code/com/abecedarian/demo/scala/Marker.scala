package com.abecedarian.demo.scala

/**
  * Created by abecedarian on 2019/4/29
  *
  */
// 私有构造方法
class Marker private(val color: String) {

  println("创建" + this)

  override def toString(): String = "颜色标记：" + color

}

//伴生对象,与类名字相同,可以访问类的私有属性和方法

object Marker {


  def apply(color: String) = {
    println("apply start")
    if (markers.contains(color)) markers(color) else null
  }

  private val markers: Map[String, Marker] = Map(
    "red" -> new Marker("red"),
    "blue" -> new Marker("blue"),
    "green" -> new Marker("green")
  )

  def getMarker(color: String) = {
    println("getMarker start")
    if (markers.contains(color)) markers(color) else null
  }

  def main(args: Array[String]) {
    println(Marker("dd"))
    println(Marker("red"))
    println(Marker("green"))
    // 单例函数调用，省略了.(点)符号
    println(Marker getMarker "blue")
  }
}


