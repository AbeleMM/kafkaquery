package org.codefeedr.kafkatime.transforms

import java.nio.ByteBuffer

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.io.FastReaderBuilder.RecordReader.Stage
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.codefeedr.kafkatime.transforms.SchemaConverter.getNestedSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

import scala.language.higherKinds

class SchemaConverterTest extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Nested(someValue: Int)

  case class someStage(someStage: Stage)

  case class AllSupportedTypes(someString: String, someFloat: Float, someDouble: Double, someInt: Int,
                               someBoolean: Boolean, someLong: Long, someOptional: Option[String], someByte: ByteBuffer,
                               someMap: Map[String, Int], someArray: Array[Int], someList: List[Long], someNested: Nested,
                               someStringList: List[String], someBooleanList: List[Boolean], someDoubleList: List[Double],
                               someByteList: List[ByteBuffer], someFloatList: List[Float], nestedStringList: List[List[String]],
                               nestedBooleanList: List[List[Boolean]], nestedFloatList: List[List[Float]],
                               nestedIntegerList: List[List[Int]], nestedByteList: List[List[ByteBuffer]],
                               nestedDoubleList: List[List[Double]], nestedLongList: List[List[Long]],
                               someNestedList: List[Nested], someNull: Null)

  def getSchema: Schema = {
    implicit object ItemToSchema extends SchemaFor[Null] {
      override def schema(fieldMapper: FieldMapper): Schema = {
        Schema.create(Schema.Type.NULL)
      }
    }
    implicit object NullEncoder extends Encoder[Null] {
      override def encode(t: Null,
                          schema: Schema,
                          fieldMapper: FieldMapper): AnyRef = {
        t
      }
    }
    implicit object DateDecoder extends Decoder[Null] {
      override def decode(value: Any,
                          schema: Schema,
                          fieldMapper: FieldMapper): Null = {
        null
      }
    }
    AvroSchema[AllSupportedTypes]
  }
  val schema: Schema = getSchema

  val testData: TableFor2[TypeInformation[_], String] =
    Table(
      ("expectedType", "FieldName"),
      (Types.STRING, "someString"),
      (Types.FLOAT, "someFloat"),
      (Types.DOUBLE, "someDouble"),
      (Types.INT, "someInt"),
      (Types.BOOLEAN, "someBoolean"),
      (Types.LONG, "someLong"),
      (Types.STRING, "someOptional"),
      (Types.BYTE, "someByte"),
      (Types.MAP(Types.STRING, Types.INT), "someMap"),
      (Types.PRIMITIVE_ARRAY(Types.INT), "someArray"),
      (Types.PRIMITIVE_ARRAY(Types.LONG), "someList"),
      (Types.ROW_NAMED(Array[String] {
        "someValue"
      }, Types.INT), "someNested"),
      (Types.LIST(Types.STRING), "someStringList"),
      (Types.PRIMITIVE_ARRAY(Types.BOOLEAN), "someBooleanList"),
      (Types.PRIMITIVE_ARRAY(Types.DOUBLE), "someDoubleList"),
      (Types.PRIMITIVE_ARRAY(Types.BYTE), "someByteList"),
      (Types.PRIMITIVE_ARRAY(Types.FLOAT), "someFloatList"),
      (Types.LIST(Types.LIST(Types.STRING)), "nestedStringList"),
      (Types.LIST(Types.LIST(Types.INT)), "nestedIntegerList"),
      (Types.LIST(Types.LIST(Types.FLOAT)), "nestedFloatList"),
      (Types.LIST(Types.LIST(Types.DOUBLE)), "nestedDoubleList"),
      (Types.LIST(Types.LIST(Types.BOOLEAN)), "nestedBooleanList"),
      (Types.LIST(Types.LIST(Types.BYTE)), "nestedByteList"),
      (Types.LIST(Types.LIST(Types.LONG)), "nestedLongList"),
      (Types.OBJECT_ARRAY(Types.ROW_NAMED(Array[String] {
        "someValue"
      }, Types.INT)), "someNestedList"),
      (Types.GENERIC(classOf[Null]), "someNull")
    )

  /**
   * Parameterized good weather tests for all supported types.
   */
  forAll(testData) { (t: TypeInformation[_], name: String) =>
    assertResult((name, t)) {
      getNestedSchema(name, schema.getField(name).schema())
    }
  }
  /**
   * Unsupported type.
   */
  assertThrows[RuntimeException] {
    getNestedSchema("doesnt matter", AvroSchema[someStage])
  }

}
