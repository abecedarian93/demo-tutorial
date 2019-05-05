package com.abecedarian.demo.scala.rpc

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcEndpoint, RpcEnv, RpcEnvServerConfig}

/**
  * Created by abecedarian on 2019/4/30
  *
  */
object HelloworldServer {

  def main(args: Array[String]): Unit = {
    val config = RpcEnvServerConfig(new RpcConf(), "hello-server", "localhost", 52345)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("hello-server", helloEndpoint)
    rpcEnv.awaitTermination()
  }


}
