package org.codefeedr.kafkatime.transforms

import java.time.Duration
import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder.TypeBuilder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._

object JsonToAvroSchema {

  def inferSchema(json: String, name: String) : Schema = {
    inferSchema(new ObjectMapper().readTree(json), SchemaBuilder.builder(), name)
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


  def retrieveLatestRecordFromTopic(topicName : String, kafkaAddress : String) : String = {
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

    for (elem <- partitions) {
      kafkaConsumer.seek(elem, kafkaConsumer.position(elem) - 1)
    }

    val records = kafkaConsumer.poll(Duration.ofMillis(100))

    records.iterator().next().value()
  }
}
