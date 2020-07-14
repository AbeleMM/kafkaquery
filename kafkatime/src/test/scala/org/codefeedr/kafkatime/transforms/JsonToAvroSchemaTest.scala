package org.codefeedr.kafkatime.transforms

import org.scalatest.funsuite.AnyFunSuite

class JsonToAvroSchemaTest extends AnyFunSuite {



  test("simple Json Conversion") {
    val jsonString = """
                       |{
                       |    "title":"noted-notes 0.1.4",
                       |    "link":"Noted is a cli note taking and todo app similar to google keep or any other note taking service. It is a fork of my previous project keep-cli. Notes appear as cards. You can make lists and write random ideas.",
                       |    "description":"https://pypi.org/project/noted-notes/0.1.4/",
                       |    "pubDate":"2020-06-14T19:42:46.000Z"
                       | }
                       """.stripMargin
    val topicName = "myTopic"
    val schema = JsonToAvroSchema.inferSchema(jsonString, topicName)

    assert(schema.toString(true) == """{
                                      |  "type" : "record",
                                      |  "name" : "myTopic",
                                      |  "fields" : [ {
                                      |    "name" : "title",
                                      |    "type" : "string"
                                      |  }, {
                                      |    "name" : "link",
                                      |    "type" : "string"
                                      |  }, {
                                      |    "name" : "description",
                                      |    "type" : "string"
                                      |  }, {
                                      |    "name" : "pubDate",
                                      |    "type" : "string"
                                      |  } ]
                                      |}""".stripMargin)
  }

  test("simple Array Conversion") {
    val jsonString = """
                       |{
                       |"colors" : [
                       |	{
                       |		"color": "red",
                       |		"value": "#f00"
                       |	},
                       |	{
                       |		"color": "magenta",
                       |		"value": "#f0f"
                       |	},
                       |	{
                       |		"color": "red",
                       |		"value": "#f00"
                       |	},
                       |	{
                       |		"color": "magenta",
                       |		"value": "#f0f"
                       |	}
                       |]
                       |}""".stripMargin
    val topicName = "myTopic"
    val schema = JsonToAvroSchema.inferSchema(jsonString, topicName)
    assert(schema.toString(true) == """{
                                      |  "type" : "record",
                                      |  "name" : "myTopic",
                                      |  "fields" : [ {
                                      |    "name" : "colors",
                                      |    "type" : {
                                      |      "type" : "array",
                                      |      "items" : {
                                      |        "type" : "record",
                                      |        "name" : "colors_type_type",
                                      |        "fields" : [ {
                                      |          "name" : "color",
                                      |          "type" : "string"
                                      |        }, {
                                      |          "name" : "value",
                                      |          "type" : "string"
                                      |        } ]
                                      |      }
                                      |    }
                                      |  } ]
                                      |}""".stripMargin)
  }
}
