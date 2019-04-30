package com.abecedarian.demo.scala

/**
  * Created by abecedarian on 2019/4/30
  *
  */

class Person(val name: String) {

  val _name = name

  def getName(): String = _name

  def apply(name: String) = println(name + " and " + _name)
}


object ApplyDemo {

  def main(args: Array[String]): Unit = {

    val v="hello scala apply"
    println(v.apply(1))
    println(v(1))

    val f=(x:Int,y:Int)=>x+y
    println(f.apply(1,2))
    println(f(1,2))

    val o = new Person("zhangsan")
    o.apply("lisi")
    o("lisi")

  }
}
