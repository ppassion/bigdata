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

case class DeviceDetail(sensorMac:String,deviceMac:String,temperature:String,dampness:String,pressure:String,date:String)
case class AlarmDevice(sensorMac:String,deviceMac:String,temperature:String)

object TemperatureCheck {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val sourceStream : DataStream[String] = env.socketTextStream("node01",9999)
    val keyedStream : KeyedStream[(DeviceDetail), String]= sourceStream.map(x => {
      val data = x.split(",")
      DeviceDetail(data(0),data(1),data(2),data(3),data(4),data(5))
    })
      .assignAscendingTimestamps(x => {format.parse(x.date).getTime})
      .keyBy(_.sensorMac)

    val pattern : Pattern[DeviceDetail,DeviceDetail] =
      Pattern.begin[DeviceDetail]("first")
        .where(x => x.temperature.toInt >= 0)
        .followedByAny("second")
        .where(x => x.temperature.toInt >= 0)
        .followedByAny("third")
        .where(x => x.temperature.toInt >= 0)
        .within(Time.seconds(180))

    val patternStream = CEP.pattern(keyedStream,pattern)

    patternStream.select(new MyTemperatureSelectFunction)

    env.execute()
  }
}

class MyTemperatureSelectFunction extends PatternSelectFunction[DeviceDetail,AlarmDevice] {
  override def select(map: util.Map[String, util.List[DeviceDetail]]): AlarmDevice = {
    val firstIterator = map.get("first").iterator()
    val secondIterator = map.get("second").iterator()
    val thirdIterator = map.get("third").iterator()

    val firstResult: DeviceDetail = firstIterator.next()
    val secondResult: DeviceDetail = secondIterator.next()
    val thirdResult: DeviceDetail = thirdIterator.next()

    println("第一条数据: "+firstResult)
    println("第二条数据: "+secondResult)
    println("第三条数据: "+thirdResult)

    //todo: 拼接高温信息
    val highTemperature=firstResult.temperature+"#"+secondResult.temperature+"#"+thirdResult.temperature
    println(highTemperature)

    //todo: 封装返回的结果信息
    AlarmDevice(thirdResult.sensorMac,thirdResult.deviceMac,highTemperature)
  }
}
