package org.codefeedr.kafkatime.transforms

import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2}

class JsonToAvroSchemaTest extends AnyFunSuite with TableDrivenPropertyChecks {

  val testData: TableFor2[String, String] =
    Table(
      ("AvroSchema", "JsonSample"),
      (
        """
          |{
          |    "type":"record",
          |    "name":"myTopic",
          |    "fields":[
          |        {
          |            "name":"title",
          |            "type":"string"
          |        },
          |        {
          |            "name":"link",
          |            "type":"string"
          |        },
          |        {
          |            "name":"description",
          |            "type":"string"
          |        },
          |        {
          |            "name":"pubDate",
          |            "type":"string"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "title":"noted-notes 0.1.4",
          |    "link":"Noted is a cli note taking and todo app.",
          |    "description":"https://pypi.org/project/noted-notes/0.1.4/",
          |    "pubDate":"2020-06-14T19:42:46.000Z"
          |}
          |""".stripMargin
      ),
      (
        """
          |{
          |    "type":"record",
          |    "name":"myTopic",
          |    "fields":[
          |        {
          |            "name":"colors",
          |            "type":{
          |                "type":"array",
          |                "items":{
          |                "type":"record",
          |                "name":"colors_type_type",
          |                "fields":[
          |                    {
          |                        "name":"color",
          |                        "type":"string"
          |                    },
          |                    {
          |                        "name":"value",
          |                        "type":"string"
          |                    }
          |                ]
          |                }
          |            }
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "colors":[
          |        {
          |            "color":"red",
          |            "value":"#f00"
          |        },
          |        {
          |            "color":"magenta",
          |            "value":"#f0f"
          |        },
          |        {
          |            "color":"red",
          |            "value":"#f00"
          |        },
          |        {
          |            "color":"magenta",
          |            "value":"#f0f"
          |        }
          |    ]
          |}
          |""".stripMargin
      ),
      (
        """
          |{
          |    "type":"record",
          |    "name":"myTopic",
          |    "fields":[
          |        {
          |            "name":"id",
          |            "type":"long"
          |        },
          |        {
          |            "name":"age",
          |            "type":"long"
          |        },
          |        {
          |            "name":"year",
          |            "type":"long"
          |        },
          |        {
          |            "name":"day",
          |            "type":"long"
          |        },
          |        {
          |            "name":"month",
          |            "type":"long"
          |        },
          |        {
          |            "name":"weight",
          |            "type":"double"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "id":1,
          |    "age":56,
          |    "year":1963,
          |    "day":3,
          |    "month":7,
          |    "weight":67.5
          |}
          |""".stripMargin
      ),
      (
        """
          |{
          |    "type":"record",
          |    "name":"myTopic",
          |    "fields":[
          |        {
          |            "name":"id",
          |            "type":"boolean"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "id":true
          |}
          |""".stripMargin
      ),
      (
        """
          |{
          |    "type":"record",
          |    "name":"myTopic",
          |    "fields":[
          |        {
          |            "name":"field_field_",
          |            "type":"string"
          |        },
          |        {
          |            "name":"_",
          |            "type":"string"
          |        },
          |        {
          |            "name":"_field",
          |            "type":"string"
          |        },
          |        {
          |            "name":"_0field",
          |            "type":"string"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "field.field/":"test1",
          |    "":"test2",
          |    "/field":"test3",
          |    "0field":"test4"
          |}
          |""".stripMargin
      )
    )

  /**
    * Parameterized good weather tests for all supported types.
    */
  forAll(testData) { (avroSchema: String, jsonSample: String) =>
    assertResult(new Schema.Parser().parse(avroSchema)) {
      val topicName = "myTopic"
      JsonToAvroSchema.inferSchema(jsonSample, topicName)
    }
  }

  val exceptionalTestData: TableFor1[String] =
    Table(
      "JsonSample",
      """
        |{
        |    "badWeather":[
        |        1,
        |        "hello"
        |    ]
        |}
        |""".stripMargin,
      """
        |{
        |    "badWeather":[
        |
        |    ]
        |}
        |""".stripMargin,
      """
        |""".stripMargin
    )

  /**
    * Parameterized bad weather tests.
    */
  forAll(exceptionalTestData) {jsonSample: String =>
    assertThrows[IllegalArgumentException] {
      val topicName = "myTopic"
      JsonToAvroSchema.inferSchema(jsonSample, topicName)
    }
  }
}
