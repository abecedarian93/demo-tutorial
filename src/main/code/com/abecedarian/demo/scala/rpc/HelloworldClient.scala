package com.abecedarian.demo.scala.rpc

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

/**
  * Created by abecedarian on 2019/4/30
  *
  */
object HelloworldClient {

  def main(args: Array[String]): Unit = {

    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52346), "hello-server")
    val result = endPointRef.askWithRetry[String](SayHi("abecedarian"))
    println(result)
  }
}
