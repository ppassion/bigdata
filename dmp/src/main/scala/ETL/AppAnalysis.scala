package ETL

import com.dmp.parentTrait.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by laowang
  */
object AppAnalysis extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val SINK_TABLE = GlobalConfigUtils.AppAnalysis
  val kuduOptions:Map[String , String] = Map(
    "kudu.master" -> KUDU_MASTER ,
    "kudu.table" -> SOURCE_TABLE
  )
  override def process( sparkSession: SparkSession, kuduContext: KuduContext): Unit = {

    //1）：获取数据:kuduMaster ， SOURCE_TABLE
    val data: DataFrame = sparkSession.read.options(kuduOptions).kudu
    data.registerTempTable("ods")
    //2)：报表
    val temp_table: DataFrame = sparkSession.sql(ContantsSQL.appAnalysis_tmp)
    temp_table.registerTempTable("temp_table")
    val result: DataFrame = sparkSession.sql(ContantsSQL.appAnalysis)

    //3）：数据落地:SINK_TABLE
    //1):schema partitionID
    val schema:Schema = ContantsSchema.APPAnalysis
    val partitionID = "appid"
    //2）：落地
    DBUtils.process(kuduContext , result , SINK_TABLE , KUDU_MASTER , schema , partitionID)
  }
}
