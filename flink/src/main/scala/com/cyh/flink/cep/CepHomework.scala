package com.cyh.flink.cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

case class HomeworkDeviceDetail(sensorMac:String,deviceMac:String,temperature:String,dampness:String,pressure:String,date:String)
case class HomeworkAlarmDevice(sensorMac:String,deviceMac:String,temperature:String)

object CepHomework {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val sourceStream : DataStream[String] = env.socketTextStream("node01",9999)
    val keyedStream : KeyedStream[(HomeworkDeviceDetail), String]= sourceStream.map(x => {
      val data = x.split(",")
      HomeworkDeviceDetail(data(0),data(1),data(2),data(3),data(4),data(5))
    })
      .assignAscendingTimestamps(x => {format.parse(x.date).getTime})
      .keyBy(_.sensorMac)

    val pattern : Pattern[HomeworkDeviceDetail,HomeworkDeviceDetail] =
      Pattern.begin[HomeworkDeviceDetail]("first")
        .where(x => x.temperature.toInt >= 50)
        .followedByAny("second")
        .where(x => x.temperature.toInt >= 50)
        .followedByAny("third")
        .where(x => x.temperature.toInt >= 50)
        .within(Time.seconds(10))

    val patternStream = CEP.pattern(keyedStream,pattern)

    patternStream.select(new HomeworkSelectFunction)

    env.execute()
  }
}

class HomeworkSelectFunction extends PatternSelectFunction[HomeworkDeviceDetail,HomeworkAlarmDevice] {
  override def select(map: util.Map[String, util.List[HomeworkDeviceDetail]]): HomeworkAlarmDevice = {
    val firstIterator = map.get("first").iterator()
    val secondIterator = map.get("second").iterator()
    val thirdIterator = map.get("third").iterator()

    val firstResult: HomeworkDeviceDetail = firstIterator.next()
    val secondResult: HomeworkDeviceDetail = secondIterator.next()
    val thirdResult: HomeworkDeviceDetail = thirdIterator.next()

    println("第一条数据: "+firstResult)
    println("第二条数据: "+secondResult)
    println("第三条数据: "+thirdResult)

    //todo: 拼接高温信息
    val highTemperature=firstResult.temperature+"#"+secondResult.temperature+"#"+thirdResult.temperature
    println(highTemperature)

    //todo: 封装返回的结果信息
    HomeworkAlarmDevice(thirdResult.sensorMac,thirdResult.deviceMac,highTemperature)
  }
}
