package org.clickhouse.query

import java.sql.ResultSet
import java.sql.SQLException
import org.clickhouse.connection.ClickhouseConnection

class ClickhouseQueryExecutor : QueryExecutor {
  override fun executeQuery(query: String): List<Map<String, Any?>> {
    val connection = ClickhouseConnection.getConnection()
    val results = mutableListOf<Map<String, Any?>>()
    try {
      connection.createStatement().use { statement ->
        statement.executeQuery(query).use { resultSet ->
          results.addAll(resultSetToList(resultSet))
        }
      }
    } catch (e: SQLException) {
      throw RuntimeException("Error executing query: ${e.message}", e)
    } finally {
      connection.close()
    }
    return results
  }

  override fun executeUpdate(query: String): Int {
    val connection = ClickhouseConnection.getConnection()
    return try {
      connection.createStatement().use { statement -> statement.executeUpdate(query) }
    } catch (e: SQLException) {
      throw RuntimeException("Error executing update: ${e.message}", e)
    } finally {
      connection.close()
    }
  }

  private fun resultSetToList(rs: ResultSet): List<Map<String, Any?>> {
    val resultList = mutableListOf<Map<String, Any?>>()
    val metaData = rs.metaData
    val columnCount = metaData.columnCount

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
}
