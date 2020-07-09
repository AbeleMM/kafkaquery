package org.codefeedr.kafkatime.transforms

import java.sql.Timestamp
import java.util

import net.manub.embeddedkafka._
import org.apache.avro.Schema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.Kafka
import org.apache.flink.types.Row
import org.codefeedr.kafkatime.commands.QueryCommand
import org.codefeedr.util.schema_exposure.ZookeeperSchemaExposer
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class RegisterTest extends AnyFunSuite with EmbeddedKafka with BeforeAndAfter {

  var sink: CollectRowSink = _

  val pypiTableName: String = "pypi_releases_min"
  val pypiTableSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |   "type":"record",
      |   "name":"PyPiRelease",
      |   "namespace":"org.codefeedr.plugins.pypi.protocol.Protocol",
      |   "fields":[
      |      {
      |         "name":"title",
      |         "type":"string"
      |      },
      |      {
      |         "name":"link",
      |         "type":"string"
      |      },
      |      {
      |         "name":"description",
      |         "type":"string"
      |      },
      |      {
      |         "name":"pubDate",
      |         "type":{
      |            "type":"string",
      |            "isRowtime":true
      |         }
      |      }
      |   ]
      |}
      |""".stripMargin)

  val npmTableName: String = "npm_releases_min"
  val npmTableSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |   "type":"record",
      |   "name":"NpmRelease",
      |   "namespace":"org.codefeedr.plugins.npm.protocol.Protocol",
      |   "fields":[
      |      {
      |         "name":"name",
      |         "type":"string"
      |      },
      |      {
      |         "name":"retrieveDate",
      |         "type":{
      |            "type":"string",
      |            "isRowtime":true
      |         }
      |      }
      |   ]
      |}
      |""".stripMargin)

  val pypiMessages: List[String] = List(
    """{ "title": "title1", "link": "link1", "description": "description1", "pubDate": "2020-05-19T17:48:00.000Z" }""",
    """{ "title": "title2", "link": "link2", "description": "description2", "pubDate": "2020-05-19T17:48:00.000Z" }""",
    """{ "title": "title3", "link": "link3", "description": "description3", "pubDate": "2020-05-19T17:48:01.000Z" }""",
    """{ "title": "title4", "link": "link4", "description": "description4", "pubDate": "2020-05-19T17:48:02.000Z" }""",
    """{ "title": "title5", "link": "link5", "description": "description5", "pubDate": "2020-05-19T17:48:03.000Z" }""",
    """{ "title": "title5", "link": "link5", "description": "description5", "pubDate": "2020-05-19T17:48:05.000Z" }""",
    ""
  )

  val npmMessages: List[String] = List(
    """{ "name": "title1", "retrieveDate": "2020-05-19T17:48:00.000Z" }""",
    """{ "name": "title2", "retrieveDate": "2020-05-19T17:48:00.000Z" }""",
    """{ "name": "title3", "retrieveDate": "2020-05-19T17:48:01.000Z" }""",
    """{ "name": "title4", "retrieveDate": "2020-05-19T17:48:02.000Z" }""",
    """{ "name": "title5", "retrieveDate": "2020-05-19T17:48:03.000Z" }""",
    """{ "name": "title5", "retrieveDate": "2020-05-19T17:48:05.000Z" }""",
    ""
  )

  after {
    CollectRowSink.result.clear()
  }

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 0,
    zooKeeperPort = 0
  )

  /**
    * Run the given query and retrieve its results.
    *
    * @param query input query to run
    * @return list of rows retrieved when executing the given query
    */
  def runQuery(query: String, delay: Long = 0): java.util.ArrayList[Row] = {

    withRunningKafkaOnFoundPort(config) { implicit config =>
      // Ensure plugin schema is added to in-memory ZooKeeper instance.
      val zke = new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}")
      zke.put(pypiTableSchema, pypiTableName)
      zke.put(npmTableSchema, npmTableName)

      val stream = new QueryCommand().registerAndApply(query, s"localhost:${config.zooKeeperPort}", s"localhost:${config.kafkaPort}", new Kafka()
        .version("universal").startFromEarliest())
      stream._1.addSink(new CollectRowSink)

      if (delay == 0) {
        for (i <- 0 to 6) {
          publishStringMessageToKafka(pypiTableName, pypiMessages(i))
          publishStringMessageToKafka(npmTableName, npmMessages(i))
        }
      } else {
        new Thread {
          override def run(): Unit = {
            for (i <- 0 to 6) {
              if (pypiMessages(i).isEmpty) {
                Thread.sleep(delay)

              }
              if (npmMessages(i).isEmpty) {
                Thread.sleep(delay)
              }
              publishStringMessageToKafka(pypiTableName, pypiMessages(i))
              publishStringMessageToKafka(npmTableName, npmMessages(i))
            }
          }
        }.start()
      }
      try {
        stream._2.execute()
      } catch {
        case _: Exception =>
      }

      CollectRowSink.result
    }
  }

  test("selectOperatorForPyPi") {
    // Remove one from the size of messages as to not count empty stop message.
    assert(runQuery("SELECT * FROM pypi_releases_min").size() == pypiMessages.size - 1)

  }

  test("tumbleWindowForPyPi") {
    val res = runQuery("select count(*) from pypi_releases_min group by TUMBLE(pubDate, interval '1' second)", 5000)
    assert(res.size() == 3)
    assert(res.asScala.map(_.getField(0).asInstanceOf[Long]).sorted == List(1, 1, 2))
  }

  test("SelectHopForPyPi") {
    val res = runQuery("select count(*) from pypi_releases_min group by HOP(pubDate, interval '2' second, interval '1' second)", 5000)
    assert(res.size() == 2)
    assert(res.asScala.map(_.getField(0).asInstanceOf[Long]).sorted == List(1, 2))
  }

  test("selectTestPyPiGroup") {
    val res: java.util.ArrayList[Row] = runQuery("select HOP_START(pubDate, interval '2' second, interval '1' second), count(*) " +
      "from pypi_releases_min GROUP BY HOP(pubDate, interval '2' second, interval '1' second) HAVING count(*) > 1", 5000)
    assert(res.size == 1)
    assert(res.asScala.map(_.getField(0).asInstanceOf[Timestamp]).toSet == Set(Timestamp.valueOf("2020-05-19 17:48:00.0")))
    assert(res.asScala.map(_.getField(1).asInstanceOf[Long]) == List(2))
  }

  test("selectTestPyPiAlias") {
    val res = runQuery("SELECT title t FROM pypi_releases_min p WHERE p.link='link3'")
    assert(res.size === 1)
  }

  test("selectTestPyPiAliasFail") {
    assertThrows[ValidationException] {
      runQuery("SELECT pypi_releases_min.title FROM pypi_releases_min p WHERE title='title1'")
    }
  }

  test("selectTestPyPiSubQueryInSingleRow") {
    val res = runQuery("select title FROM pypi_releases_min where title IN (SELECT name FROM npm_releases_min WHERE name='title1')", 5000)
    assert(res.size == 1)
  }

  test("selectTestPyPiSubQueryInMultiRow") {
    val res = runQuery("select title FROM pypi_releases_min where pubDate IN (SELECT retrieveDate FROM npm_releases_min WHERE retrieveDate" +
      " BETWEEN '2020-05-19 17:48:00.0' AND '2020-05-19 17:48:05.0')", 5000)
    assert(res.size == 6)
  }

  test("extractTopicsSimple") {
    val res = new QueryCommand().extractTopics("select count(*) from cargo group by TUMBLE(pubDate, interval '10' second)",
      List("pypi_releases_min", "cargo"))
    assert(res.toSet.equals(Set("cargo")))
  }

  test("extractTopicsNewLines") {
    val res = new QueryCommand().extractTopics("SELECT tbltable1.one, tbltable1.two, tbltable2.three\nFROM tbltable1\nINNER JOIN tbltable2\nON tbltable1.one = tbltable2.three",
      List("tbltable1", "tbltable2", "tbltable3"))
    assert(res.toSet.equals(Set("tbltable1", "tbltable2")))
  }

  test("extractTopicsInnerJoin") {
    val res = new QueryCommand().extractTopics("SELECT tbltable1.one, tbltable1.two, tbltable2.three FROM tbltable1 INNER JOIN tbltable2 ON tbltable1.one = tbltable2.three",
      List("tbltable1", "tbltable2", "tbltable3"))
    assert(res.toSet.equals(Set("tbltable1", "tbltable2")))
  }

  test("extractTopicsUnion") {
    val res = new QueryCommand().extractTopics("SELECT column_name(s) FROM table1\nUNION ALL\nSELECT column_name(s) FROM table2",
      List("table1", "table2", "table3"))
    assert(res.toSet.equals(Set("table1", "table2")))
  }

  test("extractTopicsUnionFalse") {
    val res = new QueryCommand().extractTopics("SELECT column_name(s) FROM table1\nUNION ALL\nSELECT column_name(s) FROM table2",
      List("tbltable1", "age_group", "user_count"))
    assert(res.toSet.equals(Set()))
  }

  test("extractTopicsFail") {
    val res = new QueryCommand().extractTopics("SELECT tbltable1.one, tbltable1.two, tbltable2.three FROM tbltable6 INNER JOIN tbltable7 ON tbltable1.one = tbltable2.three",
      List("tbltable1", "tbltable2", "tbltable3"))
    assert(res.toSet.equals(Set()))
  }

  test("extractTopicsSimilarName") {
    val res = new QueryCommand().extractTopics("SELECT tbltable1.one, tbltable1.two, tbltable2.three FROM pypi_release INNER JOIN tbltable7 ON tbltable1.one = tbltable2.three",
      List("pypi_release", "pypi_releases_min", "tbltable3"))
    assert(res.toSet.equals(Set("pypi_release")))
  }

  test("mapFieldsSimpleQuery") {
    val res: String = new QueryCommand().mapFields("select pubDate from pypi", List(("pubDate", "rowtime0")))
    assert(res === "select rowtime0 from pypi")
  }

  test("mapFieldsMultiple") {
    val res: String = new QueryCommand().mapFields("select pubDate, pubDate from pypi", List(("pubDate", "rowtime0")))
    assert(res === "select rowtime0, rowtime0 from pypi")
  }

  test("mapFieldsMultipleMappings") {
    val res: String = new QueryCommand().mapFields("select pubDate from pypi where any_date='today'", List(("pubDate", "rowtime0"), ("any_date", "rowtime1")))
    assert(res === "select rowtime0 from pypi where rowtime1='today'")
  }

  test("mapFieldsEmbeddedField") {
    val res: String = new QueryCommand().mapFields("select count(*) from pypi_releases_min group by HOP(any_date, interval '1' day, interval '1' day)",
      List(("any_date", "rowtime2")))
    assert(res === "select count(*) from pypi_releases_min group by HOP(rowtime2, interval '1' day, interval '1' day)")
  }

  test("mapFieldsThrowException") {
    assertThrows[IllegalArgumentException] {
      new QueryCommand().mapFields("select count(*)from pypi_releases_min group by HOP(rowtime0, interval '1' rowtime0, interval '1' day)",
        List(("pubDate", "rowtime0")))
    }
  }

}

object CollectRowSink {
  val result = new util.ArrayList[Row]
}

class CollectRowSink extends SinkFunction[Row] {
  override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {
    CollectRowSink.result.add(value)
  }
}