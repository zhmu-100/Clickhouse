package org.clickhouse.utils

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull

object ClickhouseUtils {

  fun setParameters(preparedStatement: java.sql.PreparedStatement, params: List<Any?>) {
    params.forEachIndexed { index, param -> preparedStatement.setObject(index + 1, param) }
  }

  fun convertJsonElement(json: JsonElement): Any {
    if (json is kotlinx.serialization.json.JsonPrimitive) {
      return if (json.isString) {
        json.content
      } else {
        json.intOrNull ?: json.doubleOrNull ?: json.booleanOrNull ?: json.content
      }
    }
    return json.toString()
  }

  fun resultSetToList(rs: java.sql.ResultSet): List<Map<String, Any?>> {
    val metaData = rs.metaData
    val columnCount = metaData.columnCount
    val resultList = mutableListOf<Map<String, Any?>>()
    while (rs.next()) {
      val row = mutableMapOf<String, Any?>()
      for (i in 1..columnCount) {
        val columnName = metaData.getColumnLabel(i)
        row[columnName] = rs.getObject(i)
      }
      resultList.add(row)
    }
    return resultList
  }

  fun validateIdentifier(identifier: String) {
    if (!identifier.matches(Regex("^[A-Za-z0-9_]+\$"))) {
      throw RuntimeException("Invalid identifier: $identifier")
    }
  }

  fun isValidOrderBy(orderBy: String): Boolean {
    val parts = orderBy.split(",").map { it.trim() }
    val pattern = Regex("^[A-Za-z0-9_]+(\\s+(ASC|DESC))?\$", RegexOption.IGNORE_CASE)
    return parts.all { pattern.matches(it) }
  }

  fun substitutePlaceholders(condition: String, params: List<Any?>): String {
    var index = 0
    val regex = Regex("\\?")
    return regex.replace(condition) {
      if (index >= params.size) {
        throw RuntimeException("Not enough parameters for condition")
      }
      val literal = toSqlLiteral(params[index])
      index++
      literal
    }
  }

  fun toSqlLiteral(param: Any?): String {
    return when (param) {
      null -> "NULL"
      is Number -> param.toString()
      is Boolean -> if (param) "1" else "0"
      is String -> "'${param.replace("'", "''")}'"
      else -> "'${param.toString().replace("'", "''")}'"
    }
  }
}
