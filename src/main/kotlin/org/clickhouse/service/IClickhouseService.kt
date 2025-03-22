package org.clickhouse.service

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

interface IClickhouseService {
  suspend fun insert(table: String, data: List<JsonObject>): Int
  suspend fun select(
      table: String,
      columns: List<String> = listOf("*"),
      filters: Map<String, JsonElement> = emptyMap(),
      orderBy: String? = null,
      limit: Int? = null,
      offset: Int? = null
  ): List<Map<String, Any?>>

  suspend fun update(
      table: String,
      data: Map<String, JsonElement>,
      condition: String,
      conditionParams: List<JsonElement>
  ): Int

  suspend fun delete(table: String, condition: String, conditionParams: List<JsonElement>): Int
}
