package org.codefeedr.kafkatime.parsers

import java.io.File
import java.time.Duration
import java.util.Properties

import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.avro.SchemaBuilder.TypeBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException
import org.codefeedr.kafkatime.commands.QueryCommand
import org.codefeedr.kafkatime.parsers.Configurations.{Config, Mode}
import org.codefeedr.util.schema_exposure.ZookeeperSchemaExposer
import scopt.OptionParser

import scala.collection.JavaConverters._

class Parser extends OptionParser[Config]("codefeedr") {

  head("Codefeedr CLI", "1.0.0")

  opt[String]('q', "query")
    .valueName("<query>")
    .action((x, c) => {
      c.copy(mode = Mode.Query, queryConfig = c.queryConfig.copy(query = x))
    })
    .text(s"Allows querying available data sources through Flink SQL. " +
      s"query - valid Flink SQL query. More information about Flink SQL can be found at: https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html. " +
      s"Tables and their fields share the same names as those specified for the stage.")
    .children(
      opt[Int]('p', "port")
        .valueName("<port>")
        .action((x, c) => c.copy(queryConfig = c.queryConfig.copy(port = x)))
        .text(s"Writes the output data of the given query to a socket which gets created with the specified port. " +
          s"Local connection with the host can be either done with netcat or by setting up your own socket client."),
      opt[String]('k', "kafka-topic")
        .valueName("<kafka-topic>")
        .action(
          (x, c) => c.copy(queryConfig = c.queryConfig.copy(outTopic = x)))
        .text(s"Writes the output data of the given query to the specified Kafka topic. " +
          s"If the Kafka topic does not exist, it will be created. The written query results can be seen by consuming from the Kafka topic."),
      opt[Int]('t', "timeout")
        .valueName("<seconds>")
        .action((x, c) => c.copy(queryConfig = c.queryConfig.copy(timeout = x)))
        .text(
          s"Specify a timeout in seconds. Query results will only be printed within this amount of time specified."),
      opt[Unit]("from-earliest")
        .action((_, c) =>
          c.copy(queryConfig = c.queryConfig.copy(checkEarliest = true)))
        .text(s"Specify that the query output should be printed starting from the first retrievals." +
          s"If no state is specified, then the query results will be printed from EARLIEST."),
      opt[Unit]("from-latest")
        .action((_, c) =>
          c.copy(queryConfig = c.queryConfig.copy(checkLatest = true)))
        .text(
          s"Specify that the query output should be printed starting from the latest retrievals."),
      checkConfig(
        c =>
          if (c.queryConfig.checkEarliest && c.queryConfig.checkLatest)
            failure("Cannot start from earliest and latest.")
          else success),
      checkConfig(
        c =>
          if (!c.queryConfig.outTopic.isEmpty && c.queryConfig.port != -1)
            failure("Cannot write query output to both kafka-topic and port.")
          else success
      )
    )
  opt[String]("topic")
    .valueName("<topic_name>")
    .action((x, c) => c.copy(mode = Mode.Topic, topicName = x))
    .text("Provide topic schema for given topic name. All data including the field names and field types should be present.")
  opt[Unit]("topics")
    .action((_, c) => c.copy(mode = Mode.Topics))
    .text("List all topic names which have a schema stored in Zookeeper.")
  opt[(String, String)]("schema-by-string")
    .keyName("<topic_name>")
    .valueName("<avro_Schema_String>")
    .action({
      case ((topicName, schema), c) =>
        c.copy(mode = Mode.Schema, avroSchema = schema, topicName = topicName)
    })
    .text("Inserts the specified Avro Schema (as a String) into ZooKeeper for the specified topic")
  opt[(String, File)]("schema")
    .keyName("<topic_name>")
    .valueName("<avro_Schema_file>")
    .action({
      case ((topicName, schema), c) =>
        c.copy(mode = Mode.Schema,
               avroSchema = FileUtils.readFileToString(schema),
               topicName = topicName)
    })
    .text("Inserts the specified Avro Schema (contained in a file) into ZooKeeper for the specified topic")
  opt[String]("infer-schema")
    .valueName("<topic-name>")
    .action((x, c) => c.copy(mode = Mode.Infer, topicName = x))
    .text("Infers and registers the Avro schema from the last record in the specified topic.")
  opt[String]("kafka")
    .valueName("<kafka-address>")
    .action((address, config) => config.copy(kafkaAddress = address))
    .text("Sets the Kafka address.")

  opt[String]("zookeeper")
    .valueName("<ZK-address>")
    .action((address, config) => config.copy(zookeeperAddress = address))
    .text("Sets the ZooKeeper address.")
  help('h', "help")

  private var zookeeperExposer: ZookeeperSchemaExposer = _

  def parseConfig(args: Seq[String]): Option[Config] =
    super.parse(args, Config())

  def parse(args: Seq[String]): Unit = {
    parseConfig(args) match {
      case Some(config) =>
        initZookeeperExposer(config.zookeeperAddress)
        config.mode match {
          case Mode.Query  => new QueryCommand()(config)
          case Mode.Topic  => printSchema(config.topicName)
          case Mode.Topics => printTopics()
          case Mode.Schema =>
            updateSchema(config.topicName, config.avroSchema)
          case Mode.Infer => inferSchema(config.topicName, config.kafkaAddress)
          case _ =>
            Console.err.println("Command not recognized.")
        }
      case _ => Console.err.println("Unknown configuration.")
    }
  }

  /**
    * Updates the AvroSchema in Zookeeper for the specified topic
    *
    * @param topicName    topic Name
    * @param schemaString an Avro Schema in String format.
    */
  def updateSchema(topicName: String, schemaString: String): Unit = {
    var schema: Schema = null

    try {
      schema = new Schema.Parser().parse(schemaString)
    } catch {
      case _: Throwable =>
        Console.err.println("Error while parsing the given schema.")
    }
    if (schema != null)
      zookeeperExposer.put(schema, topicName)
  }

  /**
    * List all topic names stored in Zookeeper.
    */
  def printTopics(): Unit = {
    val children = zookeeperExposer.getAllChildren

    if (children.nonEmpty) {
      val sb = new StringBuilder()
      sb.append("Available topics:\n")

      children
        .slice(0, children.size - 1)
        .foreach(x => sb.append(x + ", "))
      sb.append(children.last)
      println(sb.toString())
    } else {
      Console.err.println("There are currently no topics available")
    }
  }

  /**
    * Prints the schema associated with the topic
    *
    * @param topicName name of the topic in zookeeper
    */
  def printSchema(topicName: String): Unit = {
    val schema = zookeeperExposer.get(topicName)
    if (schema.isDefined) {
      println(schema.get.toString(true))
    } else {
      Console.err.println(s"Schema of topic $topicName is not defined.")
    }
  }

  /**
    * Initialises the ZookeeperSchemaExposer.
    *
    * @return success of the initialisation
    */
  private def initZookeeperExposer(zookeeperAddress: String): Unit = {
    try {
      zookeeperExposer = new ZookeeperSchemaExposer(zookeeperAddress)
    } catch {
      case _: KeeperException.ConnectionLossException =>
        Console.err.println("Connection to ZooKeeper lost.")
        System.exit(0)
    }
  }

  def getSchemaExposer(): ZookeeperSchemaExposer = zookeeperExposer

  def setSchemaExposer(zk: ZookeeperSchemaExposer): Unit = {
    zookeeperExposer = zk
  }

  def inferSchema(topicName: String, kafkaAddress: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaAddress)
    props.put("key.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaConsumer = new KafkaConsumer[String, String](props)

    val partitions = kafkaConsumer
      .partitionsFor(topicName)
      .asScala
      .map(x => new TopicPartition(x.topic(), x.partition()))

    kafkaConsumer.assign(partitions.asJava)

    kafkaConsumer.seekToEnd(List().asJava)

    val latestPartAndPos = partitions.map(x => (x, kafkaConsumer.position(x))).maxBy(_._2)
    kafkaConsumer.seek(latestPartAndPos._1, latestPartAndPos._2 - 1)

    val records = kafkaConsumer.poll(Duration.ofMillis(100))

    val record: String = records.iterator().next().value()

    val rootNode = new ObjectMapper().readTree(record)

    val schema = inferSchema(rootNode, SchemaBuilder.builder(), topicName)

    updateSchema(topicName, schema.toString)
  }

  def inferSchema[T](node: JsonNode, schema: TypeBuilder[T], name: String): T =
    node.getNodeType match {
      case JsonNodeType.ARRAY =>
        val arraySchemas = collection.mutable.ListBuffer[Schema]()
        node.forEach(
          x =>
            arraySchemas.append(
              inferSchema(x, SchemaBuilder.builder(), name + "_type")))

        if (arraySchemas.toSet.size != 1)
          throw new IllegalArgumentException

        schema.array().items(arraySchemas.head)

      case JsonNodeType.OBJECT =>
        val newSchema = schema.record(name).fields()
        node.fields.forEachRemaining(
          x =>
            newSchema
              .name(x.getKey)
              .`type`(
                inferSchema(x.getValue,
                            SchemaBuilder.builder(),
                            x.getKey + "_type"))
              .noDefault())
        newSchema.endRecord()

      case JsonNodeType.BOOLEAN => schema.booleanType()

      case JsonNodeType.STRING | JsonNodeType.BINARY => schema.stringType()

      case JsonNodeType.NUMBER =>
        if (node.isIntegralNumber) schema.longType() else schema.doubleType()

      case _ => throw new IllegalArgumentException
    }

}
