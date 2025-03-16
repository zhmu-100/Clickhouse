package org.clickhouse.bulk

import java.sql.SQLException
import kotlinx.serialization.json.*
import org.clickhouse.connection.ClickhouseConnection

object BulkInsert {

  fun insertBulk(table: String, jsonData: String): Int {
    val jsonArray =
        try {
          Json.parseToJsonElement(jsonData).jsonArray
        } catch (e: Exception) {
          throw RuntimeException("Error parsing JSON: ${e.message}", e)
        }

    if (jsonArray.isEmpty()) {
      return 0
    }

    val firstObj = jsonArray.first().jsonObject
    val cols = firstObj.keys.toList()

    val sqlBuilder = StringBuilder("INSERT INTO $table (")
    sqlBuilder.append(cols.joinToString(", "))
    sqlBuilder.append(") VALUES ")

    val valuesList = mutableListOf<String>()
    jsonArray.forEach { jsonElement ->
      val jsonObject = jsonElement.jsonObject
      val values =
          cols.map { column ->
            val jsonValue = jsonObject[column] ?: JsonNull
            jsonValueToSqlValue(jsonValue)
          }
      val valueRow = "(${values.joinToString(", ")})"
      valuesList.add(valueRow)
    }

    sqlBuilder.append(valuesList.joinToString(", "))
    val sql = sqlBuilder.toString()

    val connection = ClickhouseConnection.getConnection()
    return try {
      connection.createStatement().use { statement -> statement.executeUpdate(sql) }
    } catch (e: SQLException) {
      throw RuntimeException("Error executing bulk insert: ${e.message}", e)
    } finally {
      connection.close()
    }
  }

  private fun jsonValueToSqlValue(jsonElement: JsonElement): String {
    return when (jsonElement) {
      is JsonNull -> "NULL"
      is JsonPrimitive -> {
        if (jsonElement.isString) {
          "'${jsonElement.content.replace("'", "''")}'"
        } else {
          jsonElement.content
        }
      }
      else -> throw RuntimeException("Unsupported JSON element: $jsonElement")
    }
  }
}
