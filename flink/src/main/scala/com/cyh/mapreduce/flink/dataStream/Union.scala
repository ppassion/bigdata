package com.cyh.mapreduce.flink.dataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

class Union {

}

object Union {
  def main(args: Array[String]): Unit = {
    val environment : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;

    val stream1 : DataStream[String] = environment.fromCollection(Array("aa","bb"))
    val stream2 : DataStream[String] = environment.fromCollection(Array("cc","dd"))

    val stream : DataStream[String] = stream1.union(stream2)

    stream.print()

    environment.execute()
  }
}
