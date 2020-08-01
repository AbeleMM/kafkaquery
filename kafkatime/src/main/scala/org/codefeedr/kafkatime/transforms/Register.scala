package org.codefeedr.kafkatime.transforms

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row
import org.codefeedr.kafkatime.transforms.SchemaConverter.getNestedSchema
import org.codefeedr.util.schema_exposure.ZookeeperSchemaExposer

import scala.collection.JavaConverters._

trait Register {

  /**
    * Register and apply the tables of the given query
    * with the ZookeeperAddress and KafkaAddress.
    *
    * @param query            the input query
    * @param zookeeperAddress address of Zookeeper specified as an environment variable. (Default address: 'localhost:2181')
    * @param kafkaAddress     address of Kafka specified as an environment variable. (Default address: 'localhost:9092')
    * @param kafka            a Kafka object which stores the start time. ('StartFromEarliest' or 'StartFromLatest')
    * @return tuple with the datastream of rows and the streamExecutionEnvironment
    */
  def registerAndApply(
      query: String,
      zookeeperAddress: String,
      kafkaAddress: String,
      kafka: Kafka): (DataStream[Row], StreamExecutionEnvironment) = {

    val zkSchemaExposer = new ZookeeperSchemaExposer(zookeeperAddress)

    val supportedFormats = zkSchemaExposer.getAllChildren
    println("Supported Plugins: " + supportedFormats)

    val requestedTopics = extractTopics(query, supportedFormats)

    println("Requested: " + requestedTopics)

    val fsEnv =
      StreamExecutionEnvironment.getExecutionEnvironment

    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val fsTableEnv = StreamTableEnvironment.create(fsEnv)

    for (topicName <- requestedTopics) {
      val result = zkSchemaExposer.get(topicName)

      if (result.isDefined) {
        val generatedSchema = generateFlinkTableSchema(result.get)

        connectEnvironmentToTopic(fsTableEnv,
                                  topicName,
                                  generatedSchema,
                                  kafkaAddress,
                                  kafka)
      }
    }

    (fsTableEnv.sqlQuery(query).toRetractStream[Row].map(_._2), fsEnv)
  }

  /**
    * Connects the table Environment to a Kafka topic. The TableEnvironment will register a table for the topic.
    *
    * @param tableEnv         the TableEnvironment to connect
    * @param topicName        the Kafka topic name
    * @param schema           the Schema of the data within the topic
    * @param kafkaAddress     address of Kafka specified as an environment variable. (Default address: 'localhost:9092')
    * @param kafka            a Kafka object which stores the start time. ('StartFromEarliest' or 'StartFromLatest')
    */
  def connectEnvironmentToTopic(tableEnv: StreamTableEnvironment,
                                topicName: String,
                                schema: Schema,
                                kafkaAddress: String,
                                kafka: Kafka): Unit = {
    tableEnv
      .connect(
        kafka
          .version("universal")
          .topic(topicName)
          .property("bootstrap.servers", kafkaAddress)
      )
      .withFormat(
        new Json()
          .failOnMissingField(false)
          .deriveSchema()
      )
      .withSchema(
        schema
      )
      .inAppendMode()
      .registerTableSource(topicName)
  }

  /**
    * Extract the table names from the query and return the ones that are supported.
    *
    * @param query            sql query to be matched
    * @param supportedPlugins list of supported plugins from zookeeper
    * @return the subset of supported plugins
    */
  def extractTopics(query: String,
                    supportedPlugins: List[String]): List[String] = {
    supportedPlugins.intersect(query.split("\\s+|,|;|\\(|\\)"))
  }

  /**
    * Generate a Flink table schema from an Avro Schema.
    *
    * @param avroSchema  the Avro Schema
    * @return the Flink table descriptor Schema
    */
  def generateFlinkTableSchema(avroSchema: org.apache.avro.Schema): Schema = {
    val generatedSchema = new Schema()

    val rowtimeEnabled = !"false".equals(avroSchema.getProp("rowtime"))

    var rowtimeFound = false

    for (field <- avroSchema.getFields.asScala) {
      val tableInfo = getNestedSchema(field.name(), field.schema())

      if (rowtimeEnabled && !rowtimeFound && "true".equals(
            field.getProp("rowtime"))) {
        rowtimeFound = true
        generatedSchema
          .field(tableInfo._1 + "_", Types.SQL_TIMESTAMP)
          .rowtime(
            new Rowtime()
              .timestampsFromField(tableInfo._1)
              .watermarksPeriodicBounded(20))
      } else {
        generatedSchema.field(tableInfo._1, tableInfo._2)
      }
    }

    if (rowtimeEnabled && !rowtimeFound) {
      generatedSchema
        .field("kafka_time", Types.SQL_TIMESTAMP)
        .rowtime(
          new Rowtime().timestampsFromSource().watermarksPeriodicBounded(20))
    }

    generatedSchema
  }

}
