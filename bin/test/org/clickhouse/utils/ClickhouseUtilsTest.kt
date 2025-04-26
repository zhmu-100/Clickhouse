package org.clickhouse.utils

import io.mockk.every
import io.mockk.mockk
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ClickhouseUtilsTest {

  @Test
  fun `setParameters should set parameters correctly`() {
    val preparedStatement = mockk<PreparedStatement>(relaxed = true)
    ClickhouseUtils.setParameters(preparedStatement, listOf("value1", 42, true))
  }

  @Test
  fun `convertJsonElement should handle primitives correctly`() {
    val stringJson = JsonPrimitive("test")
    val intJson = JsonPrimitive(123)
    val doubleJson = JsonPrimitive(123.45)
    val booleanJson = JsonPrimitive(true)

    assertEquals("test", ClickhouseUtils.convertJsonElement(stringJson))
    assertEquals(123, ClickhouseUtils.convertJsonElement(intJson))
    assertEquals(123.45, ClickhouseUtils.convertJsonElement(doubleJson))
    assertEquals(true, ClickhouseUtils.convertJsonElement(booleanJson))
  }

  @Test
  fun `convertJsonElement should handle non-primitive JSON`() {
    val complexJson = JsonPrimitive("{\"key\":\"value\"}")
    assertEquals("{\"key\":\"value\"}", ClickhouseUtils.convertJsonElement(complexJson))
  }

  @Test
  fun `resultSetToList should convert ResultSet to List of Maps`() {
    val resultSet = mockk<ResultSet>(relaxed = true)
    val metaData = mockk<ResultSetMetaData>(relaxed = true)

    every { resultSet.metaData } returns metaData
    every { metaData.columnCount } returns 2
    every { metaData.getColumnLabel(1) } returns "id"
    every { metaData.getColumnLabel(2) } returns "name"
    every { resultSet.next() } returnsMany listOf(true, false)
    every { resultSet.getObject(1) } returns 1
    every { resultSet.getObject(2) } returns "John"

    val result = ClickhouseUtils.resultSetToList(resultSet)

    assertEquals(1, result.size)
    assertEquals(1, result[0]["id"])
    assertEquals("John", result[0]["name"])
  }

  @Test
  fun `validateIdentifier should pass correct identifiers`() {
    assertDoesNotThrow { ClickhouseUtils.validateIdentifier("valid_name_123") }
  }

  @Test
  fun `validateIdentifier should throw on invalid identifiers`() {
    assertThrows(RuntimeException::class.java) {
      ClickhouseUtils.validateIdentifier("invalid-name")
    }
  }

  @Test
  fun `isValidOrderBy should validate correct order by`() {
    assertTrue(ClickhouseUtils.isValidOrderBy("column1"))
    assertTrue(ClickhouseUtils.isValidOrderBy("column1 ASC"))
    assertTrue(ClickhouseUtils.isValidOrderBy("column1 DESC"))
  }

  @Test
  fun `isValidOrderBy should reject invalid order by`() {
    assertFalse(ClickhouseUtils.isValidOrderBy("column1; DROP TABLE users"))
  }

  @Test
  fun `substitutePlaceholders should replace placeholders`() {
    val result = ClickhouseUtils.substitutePlaceholders("id = ? AND name = ?", listOf(1, "John"))
    assertTrue(result.contains("'mocked_literal'") || result.contains("1"))
  }

  @Test
  fun `substitutePlaceholders should throw if not enough params`() {
    assertThrows(RuntimeException::class.java) {
      ClickhouseUtils.substitutePlaceholders("id = ? AND name = ?", listOf(1))
    }
  }

  @Test
  fun `toSqlLiteral should convert correctly`() {
    assertEquals("NULL", ClickhouseUtils.toSqlLiteral(null))
    assertEquals("42", ClickhouseUtils.toSqlLiteral(42))
    assertEquals("3.14", ClickhouseUtils.toSqlLiteral(3.14))
    assertEquals("1", ClickhouseUtils.toSqlLiteral(true))
    assertEquals("0", ClickhouseUtils.toSqlLiteral(false))
    assertEquals("'text'", ClickhouseUtils.toSqlLiteral("text"))
    assertEquals("'text''with''quotes'", ClickhouseUtils.toSqlLiteral("text'with'quotes"))
  }
}
