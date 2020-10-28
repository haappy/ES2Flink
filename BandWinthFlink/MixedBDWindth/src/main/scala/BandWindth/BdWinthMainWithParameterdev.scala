package BandWindth

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author 37
 * @date 2020/10/23
 */
object BdWinthMainWithParameterdev {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers" , "172.16.106.152:9092,172.16.106.153:9092,172.16.106.151:9092")
    properties.setProperty("group.id"          , "kafka_group_id40")  //从37改成39
    properties.setProperty("key.deserializer"  , "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //properties.setProperty("auto.offset.reset" , "latest")
    properties.setProperty("auto.offset.reset" , "earliest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.setParallelism(1) //local run
    //env.socketTextStream("localhost", 7777)
      env.addSource(new FlinkKafkaConsumer[String]("log2flink", new SimpleStringSchema(), properties))
      .uid("kafkaSource")
      .filter(new LogFilter)
      .map(new RepairMapper)
      .name("RepairMapper")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ESLogFormat](Time.minutes(1)) {
        override def extractTimestamp(element: ESLogFormat): Long = {
          val timestamp = element.Timestamp.substring(0, 19)
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          val parse = LocalDateTime.parse(timestamp, formatter)
          LocalDateTime.from(parse).atZone(ZoneId.systemDefault).toInstant.toEpochMilli
        }
      })
      .keyBy(lines => (lines.AppId, lines.Url))
      .timeWindow(Time.minutes(5))
      .process(new ProcessWindowFunc())
      .uid("process")
      .addSink(new BDDataSinkParame())
      .uid("BDDataSink")

    env.execute("BDWindth_dev")
  }

}
