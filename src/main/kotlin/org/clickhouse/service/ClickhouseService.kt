package org.clickhouse.service

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import org.clickhouse.connection.ClickhouseConnection
import org.clickhouse.utils.ClickhouseUtils

class ClickhouseService : IClickhouseService {

  override fun insert(table: String, data: List<JsonObject>): Int {
    if (data.isEmpty()) return 0

    ClickhouseUtils.validateIdentifier(table)
    val columns = data.first().keys.toList()
    columns.forEach { ClickhouseUtils.validateIdentifier(it) }

    val columnsJoined = columns.joinToString(", ")
    val placeholdersPerRow = "(" + columns.joinToString(", ") { "?" } + ")"
    val placeholders = List(data.size) { placeholdersPerRow }.joinToString(", ")
    val sql = "INSERT INTO $table ($columnsJoined) VALUES $placeholders"

    val params = mutableListOf<Any?>()
    data.forEach { row ->
      columns.forEach { col ->
        val jsonElem = row[col] ?: JsonNull
        params.add(ClickhouseUtils.convertJsonElement(jsonElem))
      }
    }

    val connection = ClickhouseConnection.getConnection()
    return try {
      connection.prepareStatement(sql).use { preparedStatement ->
        ClickhouseUtils.setParameters(preparedStatement, params)
        preparedStatement.executeUpdate()
      }
    } catch (e: Exception) {
      throw RuntimeException("Error executing insert: ${e.message}", e)
    } finally {
      connection.close()
    }
  }

  override fun select(
      table: String,
      columns: List<String>,
      filters: Map<String, JsonElement>,
      orderBy: String?,
      limit: Int?,
      offset: Int?
  ): List<Map<String, Any?>> {
    ClickhouseUtils.validateIdentifier(table)
    columns.forEach { if (it != "*") ClickhouseUtils.validateIdentifier(it) }
    val columnsPart =
        if (columns.isEmpty() || (columns.size == 1 && columns[0] == "*")) "*"
        else columns.joinToString(", ")
    val sqlBuilder = StringBuilder("SELECT $columnsPart FROM $table")
    val params = mutableListOf<Any?>()

    if (filters.isNotEmpty()) {
      val conditions =
          filters.entries.joinToString(" AND ") { entry ->
            ClickhouseUtils.validateIdentifier(entry.key)
            "${entry.key} = ?"
          }
      sqlBuilder.append(" WHERE $conditions")
      filters.values.forEach { params.add(ClickhouseUtils.convertJsonElement(it)) }
    }

    orderBy?.let {
      if (!ClickhouseUtils.isValidOrderBy(it)) {
        throw RuntimeException("Invalid orderBy clause")
      }
      sqlBuilder.append(" ORDER BY $it")
    }

    limit?.let {
      sqlBuilder.append(" LIMIT ?")
      params.add(it)
    }

    offset?.let {
      sqlBuilder.append(" OFFSET ?")
      params.add(it)
    }

    val sql = sqlBuilder.toString()

    val connection = ClickhouseConnection.getConnection()
    return try {
      connection.prepareStatement(sql).use { preparedStatement ->
        ClickhouseUtils.setParameters(preparedStatement, params)
        preparedStatement.executeQuery().use { resultSet ->
          ClickhouseUtils.resultSetToList(resultSet)
        }
      }
    } catch (e: Exception) {
      throw RuntimeException("Error executing select: ${e.message}", e)
    } finally {
      connection.close()
    }
  }

  override fun update(
      table: String,
      data: Map<String, JsonElement>,
      condition: String,
      conditionParams: List<JsonElement>
  ): Int {
    ClickhouseUtils.validateIdentifier(table)
    data.keys.forEach { ClickhouseUtils.validateIdentifier(it) }
    val setClause =
        data.entries.joinToString(", ") { entry ->
          "${entry.key} = ${ClickhouseUtils.toSqlLiteral(ClickhouseUtils.convertJsonElement(entry.value))}"
        }
    val substitutedCondition =
        ClickhouseUtils.substitutePlaceholders(
            condition, conditionParams.map { ClickhouseUtils.convertJsonElement(it) })
    val sql = "ALTER TABLE $table UPDATE $setClause WHERE $substitutedCondition"

    val connection = ClickhouseConnection.getConnection()
    return try {
      connection.createStatement().use { statement -> statement.executeUpdate(sql) }
    } catch (e: Exception) {
      throw RuntimeException("Error executing update: ${e.message}", e)
    } finally {
      connection.close()
    }
  }

  override fun delete(table: String, condition: String, conditionParams: List<JsonElement>): Int {
    ClickhouseUtils.validateIdentifier(table)
    val substitutedCondition =
        ClickhouseUtils.substitutePlaceholders(
            condition, conditionParams.map { ClickhouseUtils.convertJsonElement(it) })
    val sql = "ALTER TABLE $table DELETE WHERE $substitutedCondition"

    val connection = ClickhouseConnection.getConnection()
    return try {
      connection.createStatement().use { statement -> statement.executeUpdate(sql) }
    } catch (e: Exception) {
      throw RuntimeException("Error executing delete: ${e.message}", e)
    } finally {
      connection.close()
    }
  }
}
