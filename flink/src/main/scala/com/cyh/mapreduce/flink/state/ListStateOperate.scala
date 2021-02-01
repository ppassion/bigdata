package com.cyh.mapreduce.flink.state

import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

object ListStateOperate {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //准备数据集
    env.fromCollection(List(
      (1L, 3d),
      (2L, 5d),
      (2L, 1d),
      (1L, 7d),
      (1L, 4d),
      (1L, 2d),
      (1L, 3d)
    ))
      .keyBy(_._1)
      .flatMap(new CountAverageWithList())
      .print()

    env.execute()
  }
}

class CountAverageWithList extends RichFlatMapFunction[(Long,Double),(Long,Double)] {

  private var elementsByKey : ListState[(Long,Double)] = _

  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor : ListStateDescriptor[(Long,Double)] = new ListStateDescriptor("listState",classOf[(Long,Double)])
    elementsByKey = getRuntimeContext.getListState(listStateDescriptor)
  }

  override def flatMap(in: (Long, Double), collector: Collector[(Long, Double)]): Unit = {
    val currentState : java.lang.Iterable[(Long,Double)] = elementsByKey.get()
    if (null == currentState) {
      elementsByKey.addAll(Collections.emptyList())
    }
    elementsByKey.add(in)
    val allElements : Iterator[(Long,Double)] = elementsByKey.get().iterator().asScala
    val allElementsList : List[(Long,Double)] = allElements.toList
    if (allElementsList.size > 2) {
      var count = 0l
      var sum = 0d
      for (i <- allElementsList) {
        count += 1
        sum += i._2
      }
      collector.collect(in._1,sum / count)
    }
  }
}
