package com.cyh.flink.dataStream

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector

class Connect {

}

object Connect {

  def main(args: Array[String]): Unit = {
    val environment : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;

    val stream1 : DataStream[String] = environment.fromCollection(Array("aa","bb"))
    val stream2 : DataStream[Int] = environment.fromCollection(Array(1,2))

    val stream : ConnectedStreams[String,Int] = stream1.connect(stream2)

    val result : DataStream[String] = stream.flatMap(new CoFlatMapFunction[String,Int,String] {

      override def flatMap2(in2: Int, collector: Collector[String]): Unit = {
        collector.collect("" + in2 * 2)
      }

      override def flatMap1(in1: String, collector: Collector[String]): Unit = {
        collector.collect(in1.toUpperCase())
      }
    })

    result.print()

    environment.execute()
  }


}

