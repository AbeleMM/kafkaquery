package org.codefeedr.kafkatime.transforms

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

import scala.tools.nsc.doc.base.comment.Table

class JsonToAvroSchemaTest extends AnyFunSuite with TableDrivenPropertyChecks {


  val testData: TableFor2[String, String] =
    Table(("AvroSchema", "JsonSample"),
      (
        """{
  "type" : "record",
  "name" : "myTopic",
  "fields" : [ {
    "name" : "title",
    "type" : "string"
  }, {
    "name" : "link",
    "type" : "string"
  }, {
    "name" : "description",
    "type" : "string"
  }, {
    "name" : "pubDate",
    "type" : "string"
  } ]
}""",
        """
{
    "title":"noted-notes 0.1.4",
    "link":"Noted is a cli note taking and todo app similar to google keep or any other note taking service. It is a fork of my previous project keep-cli. Notes appear as cards. You can make lists and write random ideas.",
    "description":"https://pypi.org/project/noted-notes/0.1.4/",
    "pubDate":"2020-06-14T19:42:46.000Z"
 }
                       """), (
        """{
  "type" : "record",
  "name" : "myTopic",
  "fields" : [ {
    "name" : "colors",
    "type" : {
      "type" : "array",
      "items" : {
        "type" : "record",
        "name" : "colors_type_type",
        "fields" : [ {
          "name" : "color",
          "type" : "string"
        }, {
          "name" : "value",
          "type" : "string"
        } ]
      }
    }
  } ]
}""",
        """
{
"colors" : [
	{
		"color": "red",
		"value": "#f00"
	},
	{
		"color": "magenta",
		"value": "#f0f"
	},
	{
		"color": "red",
		"value": "#f00"
	},
	{
		"color": "magenta",
		"value": "#f0f"
	}
]
}"""), (
        """{
          |  "type" : "record",
          |  "name" : "myTopic",
          |  "fields" : [ {
          |    "name" : "id",
          |    "type" : "long"
          |  }, {
          |    "name" : "age",
          |    "type" : "long"
          |  }, {
          |    "name" : "bornyear",
          |    "type" : "long"
          |  }, {
          |    "name" : "date",
          |    "type" : "long"
          |  }, {
          |    "name" : "month",
          |    "type" : "long"
          |  }, {
          |    "name" : "weight",
          |    "type" : "double"
          |  } ]
          |}""".stripMargin,
        """{
      "id": 1,
      "age": 56,
      "bornyear": 1963,
      "date": 3,
      "month": 7,
      "weight" : 67.5
    }"""), (
        """{
          |  "type" : "record",
          |  "name" : "myTopic",
          |  "fields" : [ {
          |    "name" : "id",
          |    "type" : "boolean"
          |  } ]
          |}""".stripMargin,
        """{
      "id": true
    }"""))


  /**
    * Parameterized good weather tests for all supported types.
    */
  forAll(testData) { (avroSchema: String, jsonSample: String) =>
    assertResult(avroSchema) {
      val topicName = "myTopic"
      JsonToAvroSchema.inferSchema(jsonSample, topicName).toString(true)
    }
  }

}
