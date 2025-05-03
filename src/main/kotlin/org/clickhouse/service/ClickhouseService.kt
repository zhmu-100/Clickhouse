package org.clickhouse.service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import org.clickhouse.connection.ClickhouseConnection
import org.clickhouse.utils.ClickhouseUtils
import org.clickhouse.utils.Logger
import com.mad.model.LogLevel

/**
 * Реализация [IClickhouseService] для выполнения CRUD операций над базой данных ClickHouse.
 *
 * Операции выполняются с использованием пула соединений, предоставляемого [ClickhouseConnection], и
 * утилит из [ClickhouseUtils] для валидации, преобразования JSON и работы с ResultSet.
 */
class ClickhouseService : IClickhouseService {

  init {
    Logger.init()
    Logger.logActivity("Инициализация ClickhouseService")
  }

  /**
   * Вставляет данные в таблицу.
   *
   * @param table Имя таблицы.
   * @param data Список JSON объектов, содержащих данные.
   * @return Количество вставленных записей.
   */
  override suspend fun insert(table: String, data: List<JsonObject>): Int =
      withContext(Dispatchers.IO) {
        Logger.logActivity("Начало операции вставки данных")
        
        if (data.isEmpty()) {
          Logger.logActivity("Пустой список данных для вставки")
          return@withContext 0
        }

        try {
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

          Logger.logActivity("Подготовлен SQL запрос для вставки")

          ClickhouseConnection.getConnection().use { connection ->
            connection.prepareStatement(sql).use { preparedStatement ->
              ClickhouseUtils.setParameters(preparedStatement, params)
              val result = preparedStatement.executeUpdate()
              Logger.logActivity("Выполнен запрос на вставку данных")
            }
          }
          
          Logger.logActivity("Успешно вставлены данные")
          
          data.size
        } catch (e: Exception) {
          Logger.logError(
            "Ошибка при вставке данных",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString()
          )
          throw e
        }
      }

  /**
   * Выполняет SELECT запрос к таблице.
   *
   * @param table Имя таблицы.
   * @param columns Список колонок для выборки.
   * @param filters Карта фильтров для условия WHERE.
   * @param orderBy Опциональное условие сортировки.
   * @param limit Опциональное ограничение количества записей.
   * @param offset Опциональное смещение.
   * @return Список записей в виде имя столбца - значение
   */
  override suspend fun select(
      table: String,
      columns: List<String>,
      filters: Map<String, JsonElement>,
      orderBy: String?,
      limit: Int?,
      offset: Int?
  ): List<Map<String, Any?>> =
      withContext(Dispatchers.IO) {
        Logger.logActivity("Начало операции выборки данных")
        
        try {
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

          Logger.logActivity("Подготовлен SQL запрос для выборки")

          val result = ClickhouseConnection.getConnection().use { connection ->
            connection.prepareStatement(sqlBuilder.toString()).use { statement ->
              ClickhouseUtils.setParameters(statement, params)
              statement.executeQuery().use { resultSet ->
                val resultList = ClickhouseUtils.resultSetToList(resultSet)
                Logger.logActivity("Получены результаты запроса")
                resultList
              }
            }
          }
          
          Logger.logActivity("Успешно выполнена выборка данных")
          
          result
        } catch (e: Exception) {
          Logger.logError(
            "Ошибка при выборке данных",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString()
          )
          throw e
        }
      }

  /**
   * Обновляет записи в таблице ClickHouse.
   *
   * Обновляются только те поля, которые переданы в data, путем прямой подстановки литералов.
   * Условие WHERE формируется через подстановку значений, используя утилиту
   * [ClickhouseUtils.substitutePlaceholders].
   *
   * @param table Имя таблицы.
   * @param data Карта новых значений (JSON элементы).
   * @param condition Условие WHERE с плейсхолдерами (?).
   * @param conditionParams Список параметров для условия.
   * @return Количество строк, удовлетворяющих условию (до обновления).
   */
  override suspend fun update(
      table: String,
      data: Map<String, JsonElement>,
      condition: String,
      conditionParams: List<JsonElement>
  ): Int =
      withContext(Dispatchers.IO) {
        Logger.logActivity("Начало операции обновления данных")
        
        try {
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

          Logger.logActivity("Подготовлены SQL запросы для обновления")

          val initialCount = ClickhouseConnection.getConnection().use { connection ->
            val count = connection.createStatement().use { statement ->
              statement.executeQuery(countSql).use { rs ->
                if (rs.next()) rs.getInt("cnt") else 0
              }
            }
            
            Logger.logActivity("Получено количество записей для обновления")
            
            connection.createStatement().use { 
              it.executeUpdate(sql)
              Logger.logActivity("Выполнен запрос на обновление данных")
            }
            
            count
          }
          
          Logger.logActivity("Успешно обновлены данные")
          
          initialCount
        } catch (e: Exception) {
          Logger.logError(
            "Ошибка при обновлении данных",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString()
          )
          throw e
        }
      }

  /**
   * Удаляет записи из таблицы ClickHouse.
   *
   * Условие WHERE формируется путем подстановки литералов через
   * [ClickhouseUtils.substitutePlaceholders].
   *
   * @param table Имя таблицы.
   * @param condition Условие WHERE с подстановочными знаками (?).
   * @param conditionParams Список параметров для условия.
   * @return Если 0 то гуд, иначе ошибка.
   */
  override suspend fun delete(
      table: String,
      condition: String,
      conditionParams: List<JsonElement>
  ): Int =
      withContext(Dispatchers.IO) {
        Logger.logActivity("Начало операции удаления данных")
        
        try {
          ClickhouseUtils.validateIdentifier(table)

          val substitutedCondition =
              ClickhouseUtils.substitutePlaceholders(
                  condition, conditionParams.map(ClickhouseUtils::convertJsonElement))
          val sql = "ALTER TABLE $table DELETE WHERE $substitutedCondition"

          Logger.logActivity("Подготовлен SQL запрос для удаления")

          val result = ClickhouseConnection.getConnection().use { connection ->
            connection.createStatement().use { 
              val updateResult = it.executeUpdate(sql)
              Logger.logActivity("Выполнен запрос на удаление данных")
              updateResult
            }
          }
          
          Logger.logActivity("Успешно удалены данные")
          
          result
        } catch (e: Exception) {
          Logger.logError(
            "Ошибка при удалении данных",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString()
          )
          throw e
        }
      }
}
