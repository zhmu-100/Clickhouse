package org.clickhouse.utils

import com.mad.client.LoggerClient
import com.mad.model.LogLevel
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import org.clickhouse.connection.ClickhouseConfig

/**
 * Обертка для логгера, использующая LoggerClient из logger-lib.
 *
 * Предоставляет методы для логирования активности и ошибок. Инициализируется с настройками из
 * ClickhouseConfig.
 */
object Logger {
  private lateinit var loggerClient: LoggerClient
  private var initialized = false

  /** Инициализирует логгер с настройками из конфигурации. */
  fun init() {
    if (initialized) return

    val config = ClickhouseConfig.load()
    loggerClient =
        LoggerClient(
            host = config.redisHost,
            port = config.redisPort,
            password = config.redisPassword,
            activityChannel = config.redisActivityChannel,
            errorChannel = config.redisErrorChannel)
    initialized = true
  }

  /**
   * Логирует активность.
   *
   * @param event Событие для логирования.
   * @param userId ID пользователя (опционально).
   * @param deviceModel Модель устройства (опционально).
   * @param level Уровень логирования.
   */
  fun logActivity(
      event: String,
      userId: String? = null,
      deviceModel: String? = null,
      level: LogLevel = LogLevel.INFO
  ) {
    ensureInitialized()
    loggerClient.logActivity(event, userId, deviceModel, level)
  }

  /**
   * Логирует ошибку.
   *
   * @param event Событие для логирования.
   * @param errorMessage Сообщение об ошибке.
   * @param userId ID пользователя (опционально).
   * @param deviceModel Модель устройства (опционально).
   * @param stackTrace Стек вызовов (опционально).
   * @param level Уровень логирования.
   */
  fun logError(
      event: String,
      errorMessage: String,
      userId: String? = null,
      deviceModel: String? = null,
      stackTrace: String? = null,
      level: LogLevel = LogLevel.ERROR
  ) {
    ensureInitialized()
    loggerClient.logError(event, errorMessage, userId, deviceModel, stackTrace, level)
  }

  /** Закрывает логгер. */
  fun close() {
    if (initialized) {
      loggerClient.close()
      initialized = false
    }
  }

  private fun ensureInitialized() {
    if (!initialized) {
      init()
    }
  }
}

/**
 * Полезности..
 *
 * Предоставляет методы для:
 * - установки параметров в [PreparedStatement];
 * - преобразования [JsonElement] в нативный тип;
 * - преобразования [ResultSet] в список записей;
 * - валидации имен таблиц и колонок;
 * - проверки корректности выражения ORDER BY;
 * - подстановки литералов в условие WHERE;
 * - преобразования значения в SQL-литерал.
 */
object ClickhouseUtils {

  /**
   * Устанавливает параметры в [PreparedStatement].
   *
   * @param preparedStatement Подготовленный SQL-запрос.
   * @param params Список параметров для подстановки.
   */
  fun setParameters(preparedStatement: PreparedStatement, params: List<Any?>) {
    try {
      params.forEachIndexed { index, param -> preparedStatement.setObject(index + 1, param) }
      Logger.logActivity("Установка параметров в PreparedStatement")
    } catch (e: Exception) {
      Logger.logError(
          "Ошибка при установке параметров в PreparedStatement",
          e.message ?: "Неизвестная ошибка",
          stackTrace = e.stackTraceToString())
      throw e
    }
  }

  /**
   * Преобразует [JsonElement] в нативное значение.
   *
   * Если элемент является примитивом:
   * - Если это строка, возвращает её содержимое.
   * - Иначе пытается преобразовать в Int, Double или Boolean, если возможно.
   * - Если преобразование невозможно, возвращает строковое представление.
   *
   * @param json Элемент JSON для преобразования.
   * @return Нативное значение.
   */
  fun convertJsonElement(json: JsonElement): Any {
    try {
      val result =
          if (json is kotlinx.serialization.json.JsonPrimitive) {
            if (json.isString) {
              json.content
            } else {
              json.intOrNull ?: json.doubleOrNull ?: json.booleanOrNull ?: json.content
            }
          } else {
            json.toString()
          }

      Logger.logActivity("Преобразование JsonElement", level = LogLevel.DEBUG)

      return result
    } catch (e: Exception) {
      Logger.logError(
          "Ошибка при преобразовании JsonElement",
          e.message ?: "Неизвестная ошибка",
          stackTrace = e.stackTraceToString())
      throw e
    }
  }

  /**
   * Преобразует [ResultSet] в список записей.
   *
   * Каждая запись представлена в виде [Map], где ключ – имя столбца, а значение – объект,
   * полученный из [ResultSet].
   *
   * @param rs Результирующий набор данных.
   * @return Список записей.
   */
  fun resultSetToList(rs: ResultSet): List<Map<String, Any?>> {
    try {
      val metaData = rs.metaData
      val columnCount = metaData.columnCount
      val resultList = mutableListOf<Map<String, Any?>>()

      var rowCount = 0
      while (rs.next()) {
        val row = mutableMapOf<String, Any?>()
        for (i in 1..columnCount) {
          val columnName = metaData.getColumnLabel(i)
          row[columnName] = rs.getObject(i)
        }
        resultList.add(row)
        rowCount++
      }

      Logger.logActivity("Преобразование ResultSet в список")

      return resultList
    } catch (e: Exception) {
      Logger.logError(
          "Ошибка при преобразовании ResultSet в список",
          e.message ?: "Неизвестная ошибка",
          stackTrace = e.stackTraceToString())
      throw e
    }
  }

  /**
   * Проверка id. Состоит только из букв, цифр и символа подчеркивания.
   *
   * @param identifier Идентификатор для проверки.
   * @throws RuntimeException если идентификатор содержит недопустимые символы.
   */
  fun validateIdentifier(identifier: String) {
    try {
      if (!identifier.matches(Regex("^[A-Za-z0-9_]+\$"))) {
        val errorMessage = "Invalid identifier: $identifier"
        Logger.logError("Ошибка валидации идентификатора", errorMessage)
        throw RuntimeException(errorMessage)
      }

      Logger.logActivity("Валидация идентификатора", level = LogLevel.DEBUG)
    } catch (e: Exception) {
      if (e !is RuntimeException || e.message?.startsWith("Invalid identifier") != true) {
        Logger.logError(
            "Ошибка при валидации идентификатора",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString())
      }
      throw e
    }
  }

  /**
   * Проверяет, что выражение ORDER BY корректно.
   *
   * Допускается формат "имя_колонки" или "имя_колонки ASC/DESC".
   *
   * @param orderBy Выражение ORDER BY.
   * @return true, если выражение корректно, иначе false.
   */
  fun isValidOrderBy(orderBy: String): Boolean {
    try {
      val parts = orderBy.split(",").map { it.trim() }
      val pattern = Regex("^[A-Za-z0-9_]+(\\s+(ASC|DESC))?\$", RegexOption.IGNORE_CASE)
      val isValid = parts.all { pattern.matches(it) }

      Logger.logActivity("Проверка ORDER BY", level = LogLevel.DEBUG)

      return isValid
    } catch (e: Exception) {
      Logger.logError(
          "Ошибка при проверке ORDER BY",
          e.message ?: "Неизвестная ошибка",
          stackTrace = e.stackTraceToString())
      throw e
    }
  }

  /**
   * Заменяет все плейсхолдеры "?" в условии на SQL-литералы, полученные из параметров.
   *
   * @param condition Строка условия с плейсхолдерами.
   * @param params Список параметров для подстановки.
   * @return Строка условия с подставленными SQL-литералами.
   * @throws RuntimeException если количество параметров недостаточно.
   */
  fun substitutePlaceholders(condition: String, params: List<Any?>): String {
    try {
      var index = 0
      val regex = Regex("\\?")
      val result =
          regex.replace(condition) {
            if (index >= params.size) {
              val errorMessage = "Not enough parameters for condition"
              Logger.logError("Ошибка подстановки параметров", errorMessage)
              throw RuntimeException(errorMessage)
            }
            val literal = toSqlLiteral(params[index])
            index++
            literal
          }

      Logger.logActivity("Подстановка параметров в условие")

      return result
    } catch (e: Exception) {
      if (e !is RuntimeException || e.message?.startsWith("Not enough parameters") != true) {
        Logger.logError(
            "Ошибка при подстановке параметров в условие",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString())
      }
      throw e
    }
  }

  /**
   * Преобразует переданное значение в SQL-литерал.
   *
   * Если значение является числом или булевым, возвращает его строковое представление; если строка
   * — экранирует одинарные кавычки и заключает значение в одинарные кавычки.
   *
   * @param param Значение для преобразования.
   * @return SQL-литерал.
   */
  fun toSqlLiteral(param: Any?): String {
    try {
      val result =
          when (param) {
            null -> "NULL"
            is Number -> param.toString()
            is Boolean -> if (param) "1" else "0"
            is String -> "'${param.replace("'", "''")}'"
            else -> "'${param.toString().replace("'", "''")}'"
          }

      Logger.logActivity("Преобразование в SQL-литерал", level = LogLevel.DEBUG)

      return result
    } catch (e: Exception) {
      Logger.logError(
          "Ошибка при преобразовании в SQL-литерал",
          e.message ?: "Неизвестная ошибка",
          stackTrace = e.stackTraceToString())
      throw e
    }
  }
}
