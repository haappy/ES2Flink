package BandWindth

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

class BDDataSink(url:String,usr:String,pwd:String) extends RichSinkFunction[Stat]{
  private val LOG = LoggerFactory.getLogger(classOf[BDDataSink])
  var conn: Connection = _
  var insertStmt: PreparedStatement = _

  override def close(): Unit = { super.close();conn.close()}

  override def open(parameters: Configuration): Unit ={
    super.open(parameters)
    Class.forName("com.mysql.cj.jdbc.Driver")
   // conn = DriverManager.getConnection("jdbc:mysql://172.16.106.138:3306/mytest+?serverTimezone=GMT%2B8", "root", "12345678")
    conn = DriverManager.getConnection(url, usr, pwd)
    conn.setAutoCommit(false)
  }

  override def invoke(value: Stat, context: SinkFunction.Context[_]): Unit = {

    try {
      insertStmt = conn.prepareStatement("insert into  BDSum(" +
        "AppId,Url,timestamp,datatime,AllDownload,HCdnDownload,TrdCdnDownload,HcdnWaste,InternalWaste,x,AllBW,HCdnBW,CdnBW) " +
        "values(?,?,?,?,?,?,?,?,?,?,?,?,?);")
      insertStmt.setString(1,  value.bdsum.AppId)
      insertStmt.setString(2,  value.bdsum.Url)
      insertStmt.setLong(3  ,  value.bdsum.timestamp)
      insertStmt.setString(4,  value.bdsum.datatime)
      insertStmt.setLong(5,    value.bdsum.AllDownload)
      insertStmt.setLong(6,    value.bdsum.HCdnDownload)
      insertStmt.setLong(7,    value.bdsum.TrdCdnDownload)
      insertStmt.setLong(8,    value.bdsum.HcdnWaste)
      insertStmt.setLong(9,    value.bdsum.InternalWaste)
      insertStmt.setLong(10,   value.bdsum.x)
      insertStmt.setDouble(11, value.bdsum.AllBW)
      insertStmt.setDouble(12, value.bdsum.HCdnBW)
      insertStmt.setDouble(13, value.bdsum.CdnBW)
      insertStmt.executeUpdate()
    } catch {
      case e:Exception  => LOG.error("BDSum stat sink failed", e)
    }

    try {
      insertStmt = conn.prepareStatement("insert into  Uvcount (AppId,Url,timestamp,dataTime,count) values(?,?,?,?,?)")
      insertStmt.setString(1,   value.uv.Appid)
      insertStmt.setString(2,   value.uv.Url)
      insertStmt.setString(3,   value.uv.timestamp+"")
      insertStmt.setString(4,   value.uv.dataTime )
      insertStmt.setInt(   5,   value.uv.count)
      insertStmt.executeUpdate()
    } catch {
      case e:Exception  => LOG.error("Uvcount stat sink failed", e)
    }

    try {
      insertStmt =  conn.prepareStatement("insert into delaytime (AppId,Url,timestamp,datatime,startDelay,count) values(?,?,?,?,?,?);")
      insertStmt.setString(1,   value.uv.Appid)
      insertStmt.setString(2,   value.uv.Url)
      insertStmt.setString(3,   value.uv.timestamp+"")
      insertStmt.setString(4,   value.uv.dataTime )
      insertStmt.setInt(   5,   value.uv.count)
      insertStmt.executeUpdate()
    } catch {
      case e:Exception  => LOG.error("delaytime stat sink failed", e)
    }

    conn.commit()
  }
}