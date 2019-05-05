package com.abecedarian.demo.scala.rpc

import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
  * Created by abecedarian on 2019/4/30
  *
  */
class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint{

  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      println(s"receive $msg")
      context.reply(s"hi, $msg")
    }
    case SayBye(msg) => {
      println(s"receive $msg")
      context.reply(s"bye, $msg")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }
}


case class SayHi(msg: String)
case class SayBye(msg: String)
