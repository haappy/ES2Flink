package BandWindth

import org.apache.flink.api.common.functions.MapFunction
import com.alibaba.fastjson.JSON

/**
 * @author 37
 * @date 2020/10/23
 */
class RepairMapper extends MapFunction[String, ESLogFormat] {
  override def map(lines: String): ESLogFormat = {
    var str=""
    str=lines.replace("3rdCdnSpeed" , "TrdCdnSpeed")
    str=str.replace(  "3rdCdnWaste"   , "TrdCdnWaste")
    str=str.replace(  "3rdCdnDownload", "TrdCdnDownload")
    val format: ESLogFormat = JSON.parseObject(str, classOf[ESLogFormat])

    var jsonesult = format.copy()
    if (!lines.contains("Url"))               jsonesult = format.copy(Url = "mission")
    if (!lines.contains("AppId"))             jsonesult = jsonesult.copy(AppId = "mission")
    if (!lines.contains("EventType"))         jsonesult = jsonesult.copy(EventType = "mission")
    if (!lines.contains("PeerId"))            jsonesult = jsonesult.copy(PeerId = "mission")
    if (!lines.contains("Timestamp"))         jsonesult = jsonesult.copy(Timestamp = "1970-01-01 00:00:00")
    if (!lines.contains("AllDownload"))       jsonesult = jsonesult.copy(AllDownload = 0L)
    if (!lines.contains("HcdnDownload"))      jsonesult = jsonesult.copy(HcdnDownload = 0L)
    if (!lines.contains("TrdCdnDownload"))    jsonesult = jsonesult.copy(TrdCdnDownload = 0L)
    if (!lines.contains("HcdnWaste"))         jsonesult = jsonesult.copy(HcdnWaste = 0L)
    if (!lines.contains("InternalWaste"))     jsonesult = jsonesult.copy(InternalWaste = 0L)

    jsonesult
  }
}
