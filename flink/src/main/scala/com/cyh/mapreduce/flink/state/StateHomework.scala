package com.cyh.mapreduce.flink.state

import java.text.SimpleDateFormat
import java.util.{Collections, Date}

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
      .map(x => (x.split(",")(0),x.split(",")(1)))
      .keyBy(_._1)
      .flatMap(new CalTime())
      .print()

    env.execute()
  }
}

class CalTime extends RichFlatMapFunction[(String,String),(String,Long)] {

  private var elementsByKey : ListState[Date] = _

  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor : ListStateDescriptor[Date] = new ListStateDescriptor("homework",classOf[Date])
    elementsByKey = getRuntimeContext.getListState(listStateDescriptor)
  }

  override def flatMap(in: (String,String), collector: Collector[(String,Long)]): Unit = {
    val currentState : java.lang.Iterable[Date] = elementsByKey.get()
    if (null == currentState) {
      elementsByKey.addAll(Collections.emptyList())
    }
    val name:String = in._1
    val date:Date = new SimpleDateFormat("yyyy-MM-dd").parse(in._2)
    elementsByKey.add(date)
    val dateList : List[Date] = elementsByKey.get().iterator().asScala.toList
    val totalCount = dateList.size
    if(totalCount >= 3) {
      collector.collect(name,getMinGap(dateList))
    }
  }

  def getMinGap(dateList:List[Date]) : Long = {
    val newDateList = dateList.sorted
    val size = newDateList.size
    val date3 = newDateList(size - 1)
    val date2 = newDateList(size - 2)
    val date1 = newDateList(size - 3)
    val gap1 = (date2.getTime - date1.getTime)/(1000*3600*24)
    val gap2 = (date3.getTime - date2.getTime)/(1000*3600*24)
    return if(gap1 > gap2)  gap2 else gap1
  }

}
