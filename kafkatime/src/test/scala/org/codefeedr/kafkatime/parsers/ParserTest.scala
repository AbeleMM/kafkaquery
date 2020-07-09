package org.codefeedr.kafkatime.parsers

import java.io.{ByteArrayOutputStream, File, PrintWriter}

import com.sksamuel.avro4s.AvroSchema
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.codefeedr.kafkatime.parsers.Configurations.Mode
import org.codefeedr.util.schema_exposure.ZookeeperSchemaExposer
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.language.higherKinds

class ParserTest extends AnyFunSuite with EmbeddedKafka with BeforeAndAfter {

  case class testCC(s: String)

  val subjectName = "testSubject"
  var parser: Parser = _
  var outStream: ByteArrayOutputStream = _

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 0,
    zooKeeperPort = 0
  )

  val format =
    """
      |{
      |  "type" : "record",
      |  "name" : "NpmRelease",
      |  "namespace" : "org.codefeedr.plugins.npm.protocol.Protocol",
      |  "fields" : [ {
      |    "name" : "name",
      |    "type" : "string"
      |  }, {
      |    "name" : "retrieveDate",
      |    "type" : {
      |      "type" : "string",
      |      "isRowtime" : true
      |    }
      |  } ]
      |}
      |""".stripMargin

  val npmTableSchema: Schema = new Schema.Parser().parse(
    format
  )

  before {
    parser = new Parser()
    outStream = new ByteArrayOutputStream()
  }

  test("parseNothing") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      assertThrows[RuntimeException] {
        parser.parse(null)
      }
    }
  }

  test("parseDefinedPlusParseEmpty") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      parser.getSchemaExposer.put(AvroSchema[testCC], subjectName)

      parser.parse(Array("-t", subjectName))
      assert(parser.getSchemaExposer.get(subjectName).isDefined)
    }
  }

  test("printAllTopics") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      parser.getSchemaExposer.put(AvroSchema[testCC], subjectName)

      //check whether the TopicParser prints the same output after more than 1 call.
      Console.withOut(outStream)(parser.printTopics())
      val res = new String(outStream.toByteArray)
      val otherOutStream = new java.io.ByteArrayOutputStream
      Console.withOut(otherOutStream)(parser.printTopics())
      val res2 = new String(outStream.toByteArray)
      assert(res.equals(res2))
    }
  }

  test("setKafkaAddress") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      val kafkaAddress = "someAddress"
        val parserConfig = parser.parseConfig(("--kafka " + kafkaAddress + " --zookeeper \"notworkingAddress\"").split(" ")).get
      assert(parserConfig.kafkaAddress == kafkaAddress)
    }
  }

  test("setZooKeeperAddress") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      val ZKAddress = "someOTherAddress"
      val parserConfig = parser.parseConfig(("--zookeeper "+ ZKAddress).split(" ")).get
      assert(parserConfig.zookeeperAddress == ZKAddress)
    }
  }


  test("updateSchemaFromFile") {
    withRunningKafkaOnFoundPort(config) { implicit config =>

      val fileName = "schema"
      val zkAddress = s"localhost:${config.zooKeeperPort}"
      val avroSchema = """{"type":"record","name":"Person","namespace":"org.codefeedr.plugins.repl.parsers.Parser.updateSchema","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"},{"name":"city","type":"string"}]}"""
      new PrintWriter(fileName) {write(avroSchema); close}

      parser.parse(("--schema "+ subjectName +"=" + fileName+ " --zookeeper "+zkAddress).split(" "))

      assert(parser.getSchemaExposer.get(subjectName).get.toString.equals(avroSchema))

      FileUtils.deleteQuietly(new File(fileName))
    }
  }

  test("updateSchemaWithString") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      val zkAddress = s"localhost:${config.zooKeeperPort}"
      val avroSchema = "{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"org.codefeedr.plugins.repl.parsers.Parser.updateSchema\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"city\",\"type\":\"string\"}]}"
      parser.parse(("--schema-by-string "+ subjectName +"=" + avroSchema + " --zookeeper "+zkAddress).split(" "))

      assert(parser.getSchemaExposer.get(subjectName).get.toString.equals(avroSchema))
    }
  }

  test("updateSchemaParserFailure") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      val avroSchema = """incorrect Avro Format"""
      Console.withErr(outStream) {
        parser.updateSchema(subjectName, avroSchema)
        assertResult("Error while parsing the given schema.") {
          outStream.toString().trim
        }
      }
    }
  }

  test("help test") {
    Console.withOut(outStream) {

      new Parser().showUsage()
      val expectedString =
        """Codefeedr CLI 1.0.0
          |Usage: codefeedr [options]
          |
          |  -q, --query <query>      Allows querying available data sources through Flink SQL. query - valid Flink SQL query. More information about Flink SQL can be found at: https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html. Tables and their fields share the same names as those specified for the stage.
          |  -p, --port <port>        Writes the output data of the given query to a socket which gets created with the specified port. Local connection with the host can be either done with netcat or by setting up your own socket client.
          |  -k, --kafka-topic <kafka-topic>
          |                           Writes the output data of the given query to the specified Kafka topic. If the Kafka topic does not exist, it will be created. The written query results can be seen by consuming from the Kafka topic.
          |  -t, --timeout <seconds>  Specify a timeout in seconds. Query results will only be printed within this amount of time specified.
          |  --from-earliest          Specify that the query output should be printed starting from the first retrievals.If no state is specified, then the query results will be printed from EARLIEST.
          |  --from-latest            Specify that the query output should be printed starting from the latest retrievals.
          |  --topic <topic_name>     Provide topic schema for given topic name. All data including the field names and field types should be present.
          |  --topics                 List all topic names which have a schema stored in Zookeeper.
          |  --schema-by-string:<topic_name>=<avro_Schema_String>
          |                           Inserts the specified Avro Schema (as a String) into ZooKeeper for the specified topic
          |  --schema:<topic_name>=<avro_Schema_file>
          |                           Inserts the specified Avro Schema (contained in a file) into ZooKeeper for the specified topic
          |  --kafka <kafka-address>  Sets the Kafka address.
          |  --zookeeper <ZK-address>
          |                           Sets the ZooKeeper address.
          |  -h, --help""".stripMargin.trim

      assertResult(expectedString) {
        outStream.toString().trim
      }
    }
  }

  test("checkConfigQuery") {
    val args: Seq[String] = Seq(
      "-q", "select * from topic"
    )

    val parsed = parser.parseConfig(args)

    assertResult("select * from topic") {
      parsed.get.queryConfig.query
    }
    assertResult(Mode.Query) {
      parsed.get.mode
    }
  }

  test("failureOnBothFromEarliestAndLatest") {
    val args: Seq[String] = Seq(
      "-q", "select * from topic",
      "--from-earliest",
      "--from-latest"
    )

    Console.withErr(outStream) {

      parser.parseConfig(args)

      assertResult("Error: Cannot start from earliest and latest.\nTry --help for more information.") {
        outStream.toString.trim
      }
    }
  }

  test("testPrintSchema") {
    val topic = "World"

    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      parser.getSchemaExposer.put(npmTableSchema, topic)

      Console.withOut(outStream) {

        parser.printSchema(topic)

        assertResult(format.trim) {
          outStream.toString().trim
        }
      }
    }
  }

  test("testFailToPrintEmptyTopicSchema") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))

      Console.withErr(outStream) {

        parser.printSchema("Hello")

        assertResult("Schema of topic Hello is not defined.") {
          outStream.toString().trim
        }
      }
    }
  }
}