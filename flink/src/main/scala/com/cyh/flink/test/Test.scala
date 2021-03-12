package com.cyh.flink.test

object Test {
  def main(args: Array[String]): Unit = {
    val str = "ip: 192.168.56.151 , method: GET , count: 3 \n" +
      "ip: 192.168.56.151 , method: POST , count: 1 \n" +
      "ip: 192.168.56.152 , method: GET , count: 2 \n" +
      "ip: 192.168.56.152 , method: POST , count: 1 \n"
    println(str)
  }
}
