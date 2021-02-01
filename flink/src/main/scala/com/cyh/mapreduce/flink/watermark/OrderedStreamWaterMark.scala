package com.cyh.mapreduce.flink.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object OrderedStreamWaterMark {
  def main(args: Array[String]): Unit = {
    val environment : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.getConfig.setAutoWatermarkInterval(200)

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceStream : DataStream[String] = environment.socketTextStream("node01",9999)

    val mapStream : DataStream[(String,Long)] = sourceStream.map(x => (x.split(",")(0),x.split(",")(1).toLong))

    val waterMarkStream : DataStream[(String,Long)] = mapStream.assignAscendingTimestamps(x => x._2)

    waterMarkStream
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .process(new ProcessWindowFunction[(String,Long),(String,Long),Tuple,TimeWindow] {
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
          val value : String = key.getField(0)
          val startTime : Long = context.window.getStart
          val endTime : Long = context.window.getEnd
          val waterMark : Long = context.currentWatermark

          var sum:Long = 0
          val toList: List[(String,Long)] = elements.toList
          for (element <-toList) {
            sum += 1
          }
          println("窗口的数据条数:"+sum+
            " |窗口的第一条数据："+toList.head+
            " |窗口的最后一条数据："+toList.last+
            " |窗口的开始时间： "+ startTime+
            " |窗口的结束时间： "+ endTime+
            " |当前的watermark:"+ waterMark)

          out.collect(value,sum)
        }
      })

    environment.execute()
  }
}
