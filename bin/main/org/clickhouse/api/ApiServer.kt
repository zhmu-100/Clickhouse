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

/**
 * Точка входа в сервис Clickhouse.
 *
 * Настраивает и запускает Ktor сервер.
 */
fun main() {
  val config = org.clickhouse.connection.ClickhouseConfig.load()
  embeddedServer(Netty, host = config.apiHost, port = config.apiPort) { apiModule() }
      .start(wait = true)
}

/**
 * Регистрирует REST эндпоинты для CRUD операций.
 *
 * Эндпоинты:
 * - POST /insert – вставка новой записи;
 * - POST /select – выборка данных;
 * - PUT /update – обновление данных;
 * - DELETE /delete – удаление записей.
 */
fun Application.apiModule() {
  install(ContentNegotiation) { json(Json { prettyPrint = true }) }

  val service: IClickhouseService = ClickhouseService()

  routing {
    post("/insert") {
      val request = call.receive<InsertRequest>()
      val insertedRows = service.insert(request.table, request.data)
      call.respond(InsertResponse("success", insertedRows))
    }

    post("/select") {
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
    }

    put("/update") {
      val request = call.receive<UpdateRequest>()
      val affectedRows =
          service.update(request.table, request.data, request.condition, request.conditionParams)
      call.respond(UpdateResponse("success", affectedRows))
    }

    delete("/delete") {
      val request = call.receive<DeleteRequest>()
      val error = service.delete(request.table, request.condition, request.conditionParams)
      call.respond(DeleteResponse("success", error))
    }
  }
}

/**
 * Модель запроса для вставки данных.
 *
 * @property table Имя таблицы для вставки.
 * @property data Список JSON-объектов, содержащих данные для вставки.
 */
@Serializable data class InsertRequest(val table: String, val data: List<JsonObject>)

/**
 * Модель запроса для выборки данных.
 *
 * @property table Имя таблицы.
 * @property columns Список колонок для выборки (по умолчанию – все).
 * @property filters Карта фильтров для условия WHERE.
 * @property orderBy Опциональное условие сортировки.
 * @property limit Опциональное ограничение количества записей.
 * @property offset Опциональное смещение выборки.
 */
@Serializable
data class SelectRequest(
    val table: String,
    val columns: List<String> = listOf("*"),
    val filters: Map<String, JsonElement> = emptyMap(),
    val orderBy: String? = null,
    val limit: Int? = null,
    val offset: Int? = null
)

/**
 * Модель запроса для обновления данных.
 *
 * @property table Имя таблицы.
 * @property data Карта новых значений (JSON-элементы).
 * @property condition Условие WHERE с подстановочными знаками (?).
 * @property conditionParams Список параметров для условия.
 */
@Serializable
data class UpdateRequest(
    val table: String,
    val data: Map<String, JsonElement>,
    val condition: String,
    val conditionParams: List<JsonElement>
)

/**
 * Модель запроса для удаления данных.
 *
 * @property table Имя таблицы.
 * @property condition Условие WHERE с подстановочными знаками (?).
 * @property conditionParams Список параметров для условия.
 */
@Serializable
data class DeleteRequest(
    val table: String,
    val condition: String,
    val conditionParams: List<JsonElement>
)

/**
 * Модель ответа при вставке данных.
 *
 * @property status Статус операции.
 * @property insertedRows Количество вставленных строк.
 */
@Serializable data class InsertResponse(val status: String, val insertedRows: Int)

/**
 * Модель ответа при выборке данных.
 *
 * @property status Статус операции.
 * @property result Список записей в виде JSON-объектов.
 */
@Serializable data class SelectResponse(val status: String, val result: List<JsonObject>)

/**
 * Модель ответа при обновлении данных.
 *
 * @property status Статус операции.
 * @property affectedRows Количество затронутых строк.
 */
@Serializable data class UpdateResponse(val status: String, val affectedRows: Int)

/**
 * Модель ответа при удалении данных.
 *
 * @property status Статус операции.
 * @property error Если 0 - все гуд.
 */
@Serializable data class DeleteResponse(val status: String, val error: Int)

fun Map<String, Any?>.toJsonObject(): JsonObject {
  val content = mapValues { (_, value) ->
    when (value) {
      null -> JsonNull
      is Number -> JsonPrimitive(value)
      is Boolean -> JsonPrimitive(value)
      else -> JsonPrimitive(value.toString())
    }
  }
  return JsonObject(content)
}
