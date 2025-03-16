package org.clickhouse.query

interface QueryExecutor {
  fun executeQuery(query: String): List<Map<String, Any?>>

  fun executeUpdate(query: String): Int
}
