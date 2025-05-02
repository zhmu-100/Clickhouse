package org.clickhouse.utils

import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull

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
    params.forEachIndexed { index, param -> preparedStatement.setObject(index + 1, param) }
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
    if (json is kotlinx.serialization.json.JsonPrimitive) {
      return if (json.isString) {
        json.content
      } else {
        json.intOrNull ?: json.doubleOrNull ?: json.booleanOrNull ?: json.content
      }
    }
    return json.toString()
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
    val metaData = rs.metaData
    val columnCount = metaData.columnCount
    val resultList = mutableListOf<Map<String, Any?>>()
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

  /**
   * Проверка id. Состоит только из букв, цифр и символа подчеркивания.
   *
   * @param identifier Идентификатор для проверки.
   * @throws RuntimeException если идентификатор содержит недопустимые символы.
   */
  fun validateIdentifier(identifier: String) {
    if (!identifier.matches(Regex("^[A-Za-z0-9_]+\$"))) {
      throw RuntimeException("Invalid identifier: $identifier")
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
    val parts = orderBy.split(",").map { it.trim() }
    val pattern = Regex("^[A-Za-z0-9_]+(\\s+(ASC|DESC))?\$", RegexOption.IGNORE_CASE)
    return parts.all { pattern.matches(it) }
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
    var index = 0
    val regex = Regex("\\?")
    return regex.replace(condition) {
      if (index >= params.size) {
        throw RuntimeException("Not enough parameters for condition")
      }
      val literal = toSqlLiteral(params[index])
      index++
      literal
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
    return when (param) {
      null -> "NULL"
      is Number -> param.toString()
      is Boolean -> if (param) "1" else "0"
      is String -> "'${param.replace("'", "''")}'"
      else -> "'${param.toString().replace("'", "''")}'"
    }
  }
}
