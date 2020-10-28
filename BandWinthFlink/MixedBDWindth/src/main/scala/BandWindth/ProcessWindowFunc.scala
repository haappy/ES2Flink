package BandWindth

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.Set

class ProcessWindowFunc() extends ProcessWindowFunction[ESLogFormat,Stat,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[ESLogFormat], out: Collector[Stat]): Unit = {

    val windowEnd: Long = context.window.getEnd
    val df = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss")
    val dateTime = df.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), ZoneId.of("Asia/Shanghai")))

    val AppId= key._1
    val Url = key._2
    val accBD = new Array[Long](5)
    val uv: Set[String]=new mutable.HashSet[String]()
    var delayTimes:Int=0
    var sumDelay:Long=0

    elements.foreach(
      value=>{
        accBD(0) += {if(value.AllDownload.isNaN)    0 else value.AllDownload }
        accBD(1) += {if(value.HcdnDownload.isNaN)   0 else value.HcdnDownload}
        accBD(2) += {if(value.TrdCdnDownload.isNaN) 0 else value.TrdCdnDownload} //10
        accBD(3) += {if(value.HcdnWaste.isNaN)      0 else value.HcdnWaste}      //5
        accBD(4) += {if(value.InternalWaste.isNaN)  0 else value.InternalWaste}
        uv+=value.PeerId
        delayTimes=delayTimes+1
        sumDelay={if(value.PlayStartDelay.isNaN)  sumDelay  else sumDelay+value.PlayStartDelay}
      }
    )

    val actualFlow = accBD(1) - accBD(3) - accBD(4)
    out.collect(
      Stat     (BandwidthSum(AppId, Url, windowEnd, dateTime, accBD(0), accBD(1), accBD(2), accBD(3), accBD(4), actualFlow, accBD(0) * 1.00 / 300, accBD(1) * 1.00 / 300, actualFlow * 1.0 / 300)
        , Uvcount  (AppId, Url, windowEnd, dateTime, uv.size)
        , DelayTime(AppId, Url, windowEnd, dateTime, sumDelay, delayTimes))
    )
  }

}
