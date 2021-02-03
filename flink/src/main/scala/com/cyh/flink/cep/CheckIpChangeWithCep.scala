package com.cyh.flink.cep

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

case class UserLogin2 (ip:String, name:String, url:String, time:String)

object CheckIpChangeWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream : DataStream[String] = env.socketTextStream("node01",9999)
    val keyedStream : KeyedStream[(String, UserLogin2), String]= sourceStream.map(x => {
      val data = x.split(",")
      (data(1),UserLogin2(data(0),data(1),data(2),data(3)))
    })
      .keyBy(_._1)

    val pattern : Pattern[(String,UserLogin2),(String,UserLogin2)] =
      Pattern.begin[(String,UserLogin2)]("start")
        .where(x => x._2.name != null)
        .next("second")
      .where(new IterativeCondition[(String, UserLogin2)] {
        override def filter(t: (String, UserLogin2), context: IterativeCondition.Context[(String, UserLogin2)]): Boolean = {
          var flag = true
          val first = context.getEventsForPattern("start").iterator()
          while (first.hasNext) {
            val tuple = first.next()
            if(!tuple._2.ip.equals(t._2.ip)) {
              flag = true
            }
          }
          flag
        }
      }).within(Time.seconds(10))

    val patternStream = CEP.pattern(keyedStream,pattern)

    patternStream.select(new MyPatternSelectFunction())

    env.execute()
  }
}

class MyPatternSelectFunction extends PatternSelectFunction[(String,UserLogin2),(String,UserLogin2)] {
  override def select(map: util.Map[String, util.List[(String, UserLogin2)]]): (String, UserLogin2) = {
    val startIterator = map.get("start").iterator()
    val secondIterator = map.get("second").iterator()
    var tuple : (String,UserLogin2) = null
    if(startIterator.hasNext) {
      println("满足start模式中的数据："+startIterator.next())
    }
    if(secondIterator.hasNext) {
      tuple = secondIterator.next()
      println("满足second模式中的数据："+ tuple)
    }
    tuple
  }
}
