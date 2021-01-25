package com.cyh.streaming

import lombok.extern.slf4j.Slf4j
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

@Slf4j
object SocketCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf : SparkConf = new SparkConf().setAppName("socketCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    val socketTextStream : ReceiverInputDStream[String] = ssc.socketTextStream("node01",10103)
    val result : DStream[(String,Int)] = socketTextStream.flatMap(x => x.split(" ")).map((_,1)).reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
