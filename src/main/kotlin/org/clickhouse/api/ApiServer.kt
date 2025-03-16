package org.clickhouse.api

import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import org.clickhouse.bulk.BulkInsert
import org.clickhouse.connection.ClickhouseConfig
import org.clickhouse.query.ClickhouseQueryExecutor

fun main() {
  val config = ClickhouseConfig.load()
  embeddedServer(Netty, host = config.apiHost, port = config.apiPort) { apiModule() }
      .start(wait = true)
}

fun Application.apiModule() {
  install(ContentNegotiation) { json(Json { prettyPrint = true }) }

  routing {
    post("/insert") {
      val insertRequest = call.receive<InsertRequest>()
      val jsonData = Json.encodeToString(JsonElement.serializer(), insertRequest.data)
      val insertedRows = BulkInsert.insertBulk(insertRequest.table, jsonData)
      call.respond(InsertResponse("success", insertedRows))
    }
    post("/query") {
      val queryRequest = call.receive<QueryRequest>()
      val executor = ClickhouseQueryExecutor()
      val rawResult = executor.executeQuery(queryRequest.query)
      val result = rawResult.map { it.toJsonObject() }
      call.respond(QueryResponse("success", result))
    }
  }
}

@Serializable data class InsertRequest(val table: String, val data: JsonElement)

@Serializable data class QueryRequest(val query: String)

@Serializable data class InsertResponse(val status: String, val insertedRows: Int)

@Serializable data class QueryResponse(val status: String, val result: List<JsonObject>)

fun Map<String, Any?>.toJsonObject(): JsonObject {
  val content =
      this.mapValues { (_, value) ->
        when (value) {
          null -> JsonNull
          is Number -> JsonPrimitive(value)
          is Boolean -> JsonPrimitive(value)
          else -> JsonPrimitive(value.toString())
        }
      }
  return JsonObject(content)
}
