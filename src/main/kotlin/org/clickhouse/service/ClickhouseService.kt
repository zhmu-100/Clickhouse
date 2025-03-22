package org.clickhouse.service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import org.clickhouse.connection.ClickhouseConnection
import org.clickhouse.utils.ClickhouseUtils

class ClickhouseService : IClickhouseService {

  override suspend fun insert(table: String, data: List<JsonObject>): Int =
      withContext(Dispatchers.IO) {
        if (data.isEmpty()) return@withContext 0

        ClickhouseUtils.validateIdentifier(table)
        val columns = data.first().keys.toList()
        columns.forEach { ClickhouseUtils.validateIdentifier(it) }

        val columnsJoined = columns.joinToString(", ")
        val placeholders = data.joinToString(",") { "(" + columns.joinToString(",") { "?" } + ")" }
        val sql = "INSERT INTO $table ($columnsJoined) VALUES $placeholders"
        val params =
            data.flatMap { row ->
              columns.map { ClickhouseUtils.convertJsonElement(row[it] ?: JsonNull) }
            }

        ClickhouseConnection.getConnection().use { connection ->
          connection.prepareStatement(sql).use { preparedStatement ->
            ClickhouseUtils.setParameters(preparedStatement, params)
            preparedStatement.executeUpdate()
          }
        }
        data.size
      }

  override suspend fun select(
      table: String,
      columns: List<String>,
      filters: Map<String, JsonElement>,
      orderBy: String?,
      limit: Int?,
      offset: Int?
  ): List<Map<String, Any?>> =
      withContext(Dispatchers.IO) {
        ClickhouseUtils.validateIdentifier(table)
        columns.filter { it != "*" }.forEach(ClickhouseUtils::validateIdentifier)

        val columnsPart =
            if (columns.isEmpty() || columns == listOf("*")) "*" else columns.joinToString(", ")
        val sqlBuilder = StringBuilder("SELECT $columnsPart FROM $table")
        val params = mutableListOf<Any?>()

        if (filters.isNotEmpty()) {
          val conditions = filters.entries.joinToString(" AND ") { "${it.key} = ?" }
          filters.keys.forEach(ClickhouseUtils::validateIdentifier)
          sqlBuilder.append(" WHERE $conditions")
          params.addAll(filters.values.map(ClickhouseUtils::convertJsonElement))
        }

        orderBy?.let {
          require(ClickhouseUtils.isValidOrderBy(it)) { "Invalid orderBy clause" }
          sqlBuilder.append(" ORDER BY $it")
        }

        limit?.let {
          sqlBuilder.append(" LIMIT ?")
          params += it
        }

        offset?.let {
          sqlBuilder.append(" OFFSET ?")
          params += it
        }

        ClickhouseConnection.getConnection().use { connection ->
          connection.prepareStatement(sqlBuilder.toString()).use { statement ->
            ClickhouseUtils.setParameters(statement, params)
            statement.executeQuery().use(ClickhouseUtils::resultSetToList)
          }
        }
      }

  override suspend fun update(
      table: String,
      data: Map<String, JsonElement>,
      condition: String,
      conditionParams: List<JsonElement>
  ): Int =
      withContext(Dispatchers.IO) {
        ClickhouseUtils.validateIdentifier(table)
        data.keys.forEach(ClickhouseUtils::validateIdentifier)

        val setClause =
            data.entries.joinToString(", ") {
              "${it.key} = ${ClickhouseUtils.toSqlLiteral(ClickhouseUtils.convertJsonElement(it.value))}"
            }
        val substitutedCondition =
            ClickhouseUtils.substitutePlaceholders(
                condition, conditionParams.map(ClickhouseUtils::convertJsonElement))
        val sql = "ALTER TABLE $table UPDATE $setClause WHERE $substitutedCondition"
        val countSql = "SELECT count() AS cnt FROM $table WHERE $substitutedCondition"

        ClickhouseConnection.getConnection().use { connection ->
          val initialCount =
              connection.createStatement().use { statement ->
                statement.executeQuery(countSql).use { rs ->
                  if (rs.next()) rs.getInt("cnt") else 0
                }
              }
          connection.createStatement().use { it.executeUpdate(sql) }
          initialCount
        }
      }

  override suspend fun delete(
      table: String,
      condition: String,
      conditionParams: List<JsonElement>
  ): Int =
      withContext(Dispatchers.IO) {
        ClickhouseUtils.validateIdentifier(table)

        val substitutedCondition =
            ClickhouseUtils.substitutePlaceholders(
                condition, conditionParams.map(ClickhouseUtils::convertJsonElement))
        val sql = "ALTER TABLE $table DELETE WHERE $substitutedCondition"

        ClickhouseConnection.getConnection().use { connection ->
          connection.createStatement().use { it.executeUpdate(sql) }
        }
      }
}
