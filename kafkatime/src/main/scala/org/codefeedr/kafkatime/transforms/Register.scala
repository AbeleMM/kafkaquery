package org.codefeedr.kafkatime.transforms

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row
import SchemaConverter.getNestedSchema
import org.codefeedr.util.schema_exposure.ZookeeperSchemaExposer

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait Register {

  var rowtimeCounter = 0

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
    val executionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    executionEnvironment.setStreamTimeCharacteristic(
      TimeCharacteristic.EventTime)

    val tableEnvironment = StreamTableEnvironment.create(executionEnvironment)

    rowtimeCounter = 0
    val fieldMapper = new ListBuffer[(String, String)]

    for (topicName <- requestedTopics) {
      val result = zkSchemaExposer.get(topicName)

      if (result.isDefined) {
        val generatedSchema =
          generateFlinkTableSchema(result.get, query, fieldMapper)

        connectEnvironmentToTopic(tableEnvironment,
                                  topicName,
                                  generatedSchema,
                                  zookeeperAddress,
                                  kafkaAddress,
                                  kafka)
      }
    }

    val queryMapped = mapFields(query, fieldMapper.toList)

    (tableEnvironment.sqlQuery(queryMapped).toRetractStream[Row].map(_._2),
     executionEnvironment)
  }

  /**
    * Connects the table Environment to a Kafka topic. The TableEnvironment will register a table for the topic.
    *
    * @param tableEnv         the TableEnvironment to connect
    * @param topicName        the Kafka topic name
    * @param schema           the Schema of the data within the topic
    * @param zookeeperAddress address of Zookeeper specified as an environment variable. (Default address: 'localhost:2181')
    * @param kafkaAddress     address of Kafka specified as an environment variable. (Default address: 'localhost:9092')
    * @param kafka            a Kafka object which stores the start time. ('StartFromEarliest' or 'StartFromLatest')
    */
  def connectEnvironmentToTopic(tableEnv: StreamTableEnvironment,
                                topicName: String,
                                schema: Schema,
                                zookeeperAddress: String,
                                kafkaAddress: String,
                                kafka: Kafka): Unit = {
    tableEnv
      .connect(
        kafka
          .topic(topicName)
          .property("zookeeper.connect", zookeeperAddress)
          .property("bootstrap.servers", kafkaAddress)
      )
      .withFormat(
        new Json()
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
    * @param query       the input query
    * @param fieldMapper empty ListBuffer of string pairs containing the plugin's rowtime name
    *                    and a generated rowtime name
    * @return the Flink table descriptor Schema
    */
  def generateFlinkTableSchema(
      avroSchema: org.apache.avro.Schema,
      query: String,
      fieldMapper: ListBuffer[(String, String)]): Schema = {
    var generatedSchema = new Schema()
    for (field <- avroSchema.getFields.asScala) {
      val tableInfo = getNestedSchema(field.name(), field.schema())
      if (field.schema().getObjectProp("isRowtime").asInstanceOf[Boolean]) {
        val rowtime = "rowtime" + rowtimeCounter
        do {
          rowtimeCounter += 1
        } while (query.contains(rowtime))
        generatedSchema
          .field(rowtime,
                 org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP)
          .rowtime(
            new Rowtime()
              .timestampsFromField(tableInfo._1)
              .watermarksPeriodicBounded(2000))
        fieldMapper.+=:(tableInfo._1, rowtime)
      } else {
        generatedSchema = generatedSchema.field(tableInfo._1, tableInfo._2)
      }
    }
    generatedSchema
  }

  /**
    * The plugin rowtime parameter from the query is replaced by a generated
    * rowtime attribute recognised by the TableExecutionEnvironment.
    *
    * @param query       sql query including plugins fields
    * @param fieldMapper a list of string pairs containing the plugin's rowtime name
    *                    and the name generated by generateFlinkTableSchema
    * @return
    */
  def mapFields(query: String, fieldMapper: List[(String, String)]): String = {
    var res = query
    fieldMapper.foreach(field => {
      val reg = ("(\\W?)(" + field._1 + ")(\\W?)").r
      if (("(\\W?)(" + field._2 + ")(\\W?)").r.findFirstIn(query).isDefined)
        throw new IllegalArgumentException(
          "\"" + query + "\" already contains a the map value of" + field._1 + ".")
      reg
        .findAllMatchIn(res)
        .foreach(ins =>
          res = reg.replaceFirstIn(res, ins.group(1) + field._2 + ins.group(3)))
    })
    res
  }

}
