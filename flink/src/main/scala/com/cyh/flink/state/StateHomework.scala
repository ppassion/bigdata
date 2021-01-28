package com.cyh.flink.state

import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object StateHomework {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream : DataStream[String] = env.socketTextStream("node01",9999)
    sourceStream
      .keyBy(_._0)
      .flatMap(new CalTime())
      .print()
    //准备数据集
    env.fromCollection(List(
      ("aa", "bb"),
      ("cc", "dd")
    ))
      .keyBy(_._1)
      .flatMap(new CalTime())
      .print()

    env.execute()
  }
}

class CalTime extends RichFlatMapFunction[(String,String),(String,String)] {

  private var elementsByKey : ListState[(String,String)] = _

  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor : ListStateDescriptor[(String,String)] = new ListStateDescriptor("listState",classOf[(String,String)])
    elementsByKey = getRuntimeContext.getListState(listStateDescriptor)
  }

  override def flatMap(in: (String,String), collector: Collector[(String,String)]): Unit = {
    val currentState : java.lang.Iterable[(String,String)] = elementsByKey.get()
    if (null == currentState) {
      elementsByKey.addAll(Collections.emptyList())
    }
    elementsByKey.add(in)
    val allElements : Iterator[(String,String)] = elementsByKey.get().iterator().asScala
    val allElementsList : List[(String,String)] = allElements.toList
    collector.collect(in)
  }
}
