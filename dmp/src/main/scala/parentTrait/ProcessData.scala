package parentTrait

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by laowang on .
  */
trait ProcessData {
  def process(sparkSession: SparkSession, kuduContext: KuduContext)
}
