package com.cyh.flink.cep

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

case class UserLogin (ip:String, name:String, url:String, time:String)

object CheckIPChange {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream : DataStream[String] = env.socketTextStream("node01",9999)
    sourceStream.map(x => {
      val data = x.split(",")
      (data(1),UserLogin(data(0),data(1),data(2),data(3)))
    })
      .keyBy(_._1)
      .process(new MyLoginProcessFunction())
      .print()

    env.execute()
  }
}

class MyLoginProcessFunction extends KeyedProcessFunction[String,(String,UserLogin),(String,UserLogin)] {
  var listState : ListState[UserLogin] = _
  override def processElement(i: (String, UserLogin), context: KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)]#Context, collector: Collector[(String, UserLogin)]): Unit = {
    listState.add(i._2)
    val toList = listState.get().asScala.toList
    val sortedList : List[UserLogin] = toList.sortBy(_.time)
    if (sortedList.size == 2) {
      val first = sortedList(0)
      val second = sortedList(1)

      if( !first.ip.equals(second.ip)) {
        println("ip changed")
      }
      listState.clear()
      listState.add(second)
    }
    collector.collect(i)
  }

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[UserLogin]("changeIp",classOf[UserLogin]))
  }
}


