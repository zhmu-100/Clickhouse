package org.clickhouse.service

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * Интерфейс сервиса для работы с базой данных ClickHouse.
 *
 * Определяет следующие операции:
 * - [insert] – вставка новой записи,
 * - [select] – выборка данных,
 * - [update] – обновление записей,
 * - [delete] – удаление записей.
 */
interface IClickhouseService {
  /**
   * Вставляет новые данные в таблицу.
   *
   * @param table Имя таблицы.
   * @param data Список JSON объектов с данными для вставки. * @return Количество вставленных
   * записей.
   */
  suspend fun insert(table: String, data: List<JsonObject>): Int

  /**
   * Выполняет SELECT запрос.
   *
   * @param table Имя таблицы.
   * @param columns Список колонок для выборки (по умолчанию все).
   * @param filters Карта фильтров для условия WHERE.
   * @param orderBy Опциональное условие сортировки.
   * @param limit Опциональное ограничение количества записей.
   * @param offset Опциональное смещение.
   * @return Список записей в виде имя столбца - значение
   */
  suspend fun select(
      table: String,
      columns: List<String> = listOf("*"),
      filters: Map<String, JsonElement> = emptyMap(),
      orderBy: String? = null,
      limit: Int? = null,
      offset: Int? = null
  ): List<Map<String, Any?>>

  /**
   * Обновляет данные в таблице.
   *
   * @param table Имя таблицы.
   * @param data Карта новых значений в виде JSON элементов.
   * @param condition Условие WHERE с подстановочными знаками (?).
   * @param conditionParams Список параметров для условия.
   * @return Количество строк, затронутых обновлением.
   */
  suspend fun update(
      table: String,
      data: Map<String, JsonElement>,
      condition: String,
      conditionParams: List<JsonElement>
  ): Int

  /**
   * Удаляет записи из таблицы.
   *
   * @param table Имя таблицы.
   * @param condition Условие WHERE с подстановочными знаками (?).
   * @param conditionParams Список параметров для условия.
   * @return Если 0 то гуд, иначе ошибка.
   */
  suspend fun delete(table: String, condition: String, conditionParams: List<JsonElement>): Int
}
