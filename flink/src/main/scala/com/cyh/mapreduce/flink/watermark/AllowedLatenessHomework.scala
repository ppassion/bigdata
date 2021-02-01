package com.cyh.mapreduce.flink.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowedLatenessHomework {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceStream : DataStream[String] = env.socketTextStream("node01",9999)
    val mapStream : DataStream[(String,Long)] =
      sourceStream.map(x => (x.split(",")(0),x.split(",")(1).toLong))
    val lateTag = new OutputTag[(String,Long)]("lateData")
    val result:DataStream[(String,Long)] = mapStream
      .assignTimestampsAndWatermarks(new MyWaterMark())
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(3))
      .sideOutputLateData(lateTag)
      .process(new MyProcessFunction())

    result.getSideOutput(lateTag).print("lateData")
    result.print("ok")
    env.execute()


  }
}

class MyWaterMark extends AssignerWithPeriodicWatermarks[(String,Long)] {
  val maxOutOfOrder = 3000l
  var currentMaxTimestamp : Long = _

  override def getCurrentWatermark: Watermark = {
    val watermark = new Watermark(currentMaxTimestamp - maxOutOfOrder)
    watermark
  }

  override def extractTimestamp(t: (String, Long), l: Long): Long = {
    val currentElementEventTime : Long = t._2
    currentMaxTimestamp = Math.max(currentMaxTimestamp,currentElementEventTime)
    println("receive" + t + " time " + currentElementEventTime)
    currentElementEventTime
  }
}


class MyProcessFunction extends ProcessWindowFunction[(String,Long),(String,Long),Tuple,TimeWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
    val value = key.getField[String](0)
    val startTime = context.window.getStart
    val endTime = context.window.getEnd
    val watermark = context.currentWatermark
    var sum = 0
    val toList = elements.toList
    for (i <- toList) {
      sum += 1
    }

    println("窗口的数据条数:" + sum +
      " |窗口的第一条数据：" + toList.head +
      " |窗口的最后一条数据：" + toList.last +
      " |窗口的开始时间： " + startTime +
      " |窗口的结束时间： " + endTime +
      " |当前的watermark:" + watermark)

    out.collect((value,sum))
  }
}
