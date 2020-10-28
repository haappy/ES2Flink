package BandWindth

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
/**
 * @author 37
 * @date 2020/10/23
 */
object BdWinthMain {
  def main(args: Array[String]): Unit = {

    import org.apache.commons.cli._
    val options = new Options
    options.addOption(Option.builder.hasArg.longOpt("mysql-addr").desc("mysql addr").required.build)
      .addOption(Option.builder().hasArg().longOpt("mysql-usr").desc("mysql usr").required().build())
      .addOption(Option.builder().hasArg().longOpt("mysql-pwd").desc("mysql pwd").required().build())
      .addOption(Option.builder().hasArg().longOpt("job-name").desc("job name").required().build())
      .addOption(Option.builder().hasArg().longOpt("fs").desc("checkpoint store filesystem").build())
    val parser = new DefaultParser
    val cmd: CommandLine = parser.parse(options, args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    //env.setStateBackend(new FsStateBackend("hdfs:///log2flinkTest/checkpoint.txt"))
    env.setStateBackend(new FsStateBackend(cmd.getOptionValue("fs")))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", cmd.getOptionValue("kafka-bootstrap-servers"))
    properties.setProperty("group.id"         ,  cmd.getOptionValue("kafka-group-id"))
    properties.setProperty("key.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")



    env.addSource(new FlinkKafkaConsumer[String](cmd.getOptionValue("kafka-topic"), new SimpleStringSchema(), properties))
      .uid("kafkaSource")
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
      .addSink(new BDDataSink(cmd.getOptionValue("mysql-addr"), cmd.getOptionValue("mysql-usr"), cmd.getOptionValue("mysql-pwd")))
      .uid("BDDataSink")

    env.execute(cmd.getOptionValue("job-name"))

  }

}
