package ETL

import com.dmp.parentTrait.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by laowang
  */
object ChannelAnalysis extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val SINK_TABLE = GlobalConfigUtils.ChannelAnalysis
  val kuduOptions:Map[String , String] = Map(
    "kudu.master" -> KUDU_MASTER ,
    "kudu.table" -> SOURCE_TABLE
  )
  override def process(sparkSession: SparkSession, kuduContext: KuduContext): Unit = {
    //1:获取数据
    val data = sparkSession.read.options(kuduOptions).kudu
    data.registerTempTable("ods")
    //2:报表
    val tmp = sparkSession.sql(ContantsSQL.ChannelAnalysis_tmp)
    tmp.registerTempTable("temp_table")
    val result = sparkSession.sql(ContantsSQL.ChannelAnalysis)

    //3：数据落地
    val schema:Schema = ContantsSchema.ChannelAnalysis
    val partitionID = "channelid"
    DBUtils.process(kuduContext , result , SINK_TABLE , KUDU_MASTER , schema , partitionID)
  }
}
