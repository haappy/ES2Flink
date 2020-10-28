package BandWindth

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.FilterFunction

/**
 * @author 37
 * @date 2020/10/27
 *
 */
class LogFilter() extends FilterFunction[String]{
  override def filter(lines: String): Boolean = {
    try {
      JSON.parseObject(lines).isInstanceOf[JSON]
    }
    catch {
      case _ => println("非json")
        false
    }
  }
}
