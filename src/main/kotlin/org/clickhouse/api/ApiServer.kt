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
import org.clickhouse.service.ClickhouseService
import org.clickhouse.service.IClickhouseService

fun main() {
  val config = org.clickhouse.connection.ClickhouseConfig.load()
  embeddedServer(Netty, host = config.apiHost, port = config.apiPort) { apiModule() }
      .start(wait = true)
}

fun Application.apiModule() {
  install(ContentNegotiation) { json(Json { prettyPrint = true }) }

  val service: IClickhouseService = ClickhouseService()

  routing {
    post("/insert") {
      try {
        val request = call.receive<InsertRequest>()
        val insertedRows = service.insert(request.table, request.data)
        call.respond(InsertResponse("success", insertedRows))
      } catch (e: Exception) {
        e.printStackTrace()
        call.respondText("Error: ${e.localizedMessage}")
      }
    }
    post("/select") {
      try {
        val request = call.receive<SelectRequest>()
        val result =
            service.select(
                request.table,
                request.columns,
                request.filters,
                request.orderBy,
                request.limit,
                request.offset)
        call.respond(SelectResponse("success", result.map { it.toJsonObject() }))
      } catch (e: Exception) {
        e.printStackTrace()
        call.respondText("Error: ${e.localizedMessage}")
      }
    }
    put("/update") {
      try {
        val request = call.receive<UpdateRequest>()
        val affectedRows =
            service.update(request.table, request.data, request.condition, request.conditionParams)
        call.respond(UpdateResponse("success", affectedRows))
      } catch (e: Exception) {
        e.printStackTrace()
        call.respondText("Error: ${e.localizedMessage}")
      }
    }
    delete("/delete") {
      try {
        val request = call.receive<DeleteRequest>()
        val affectedRows = service.delete(request.table, request.condition, request.conditionParams)
        call.respond(DeleteResponse("success", affectedRows))
      } catch (e: Exception) {
        e.printStackTrace()
        call.respondText("Error: ${e.localizedMessage}")
      }
    }
  }
}

@Serializable data class InsertRequest(val table: String, val data: List<JsonObject>)

@Serializable
data class SelectRequest(
    val table: String,
    val columns: List<String> = listOf("*"),
    val filters: Map<String, JsonElement> = emptyMap(),
    val orderBy: String? = null,
    val limit: Int? = null,
    val offset: Int? = null
)

@Serializable
data class UpdateRequest(
    val table: String,
    val data: Map<String, JsonElement>,
    val condition: String,
    val conditionParams: List<JsonElement>
)

@Serializable
data class DeleteRequest(
    val table: String,
    val condition: String,
    val conditionParams: List<JsonElement>
)

@Serializable data class InsertResponse(val status: String, val insertedRows: Int)

@Serializable data class SelectResponse(val status: String, val result: List<JsonObject>)

@Serializable data class UpdateResponse(val status: String, val affectedRows: Int)

@Serializable data class DeleteResponse(val status: String, val affectedRows: Int)

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
