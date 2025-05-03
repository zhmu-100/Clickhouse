package org.clickhouse.connection

import com.mad.model.LogLevel
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.clickhouse.utils.Logger

/**
 * Объект для управления пулом соединений с базой данных ClickHouse.
 *
 * Инициализирует пул с [INITIAL_POOL_SIZE] соединений и ограничивает общее число соединений
 * [MAX_POOL_SIZE]. При запросе соединения метод [getConnection] возвращает соединение из пула или
 * создаёт новое, если пул не достиг максимума. Метод [close] закрывает все соединения в пуле.
 *
 * Соединения оборачиваются в [PooledConnection], чтобы при вызове метода [close] соединение
 * возвращалось в пул, а не закрывалось.
 */
object ClickhouseConnection {
  private const val INITIAL_POOL_SIZE = 10
  private const val MAX_POOL_SIZE = 20
  private val connectionPool = LinkedBlockingDeque<Connection>(MAX_POOL_SIZE)
  private val totalConnections = AtomicInteger(0)

  init {
    Logger.init()
    Logger.logActivity("Инициализация пула соединений ClickHouse")

    repeat(INITIAL_POOL_SIZE) {
      try {
        val conn = createNewConnection()
        connectionPool.offer(conn)
        totalConnections.incrementAndGet()
        Logger.logActivity("Создано начальное соединение с ClickHouse")
      } catch (e: Exception) {
        Logger.logError(
            "Ошибка при создании начального соединения с ClickHouse",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString())
      }
    }
  }

  /**
   * Создает новое соединение с базой данных ClickHouse, используя настройки из [ClickhouseConfig].
   *
   * @return Новое соединение [Connection].
   * @throws RuntimeException если происходит ошибка подключения.
   */
  private fun createNewConnection(): Connection {
    val config = ClickhouseConfig.load()
    val url = config.clickhouseUrl
    Logger.logActivity("Создание нового соединения с ClickHouse")

    return try {
      val connection =
          DriverManager.getConnection(url, config.clickhouseUser, config.clickhousePassword)
      Logger.logActivity("Соединение с ClickHouse успешно создано")
      connection
    } catch (e: SQLException) {
      val errorMessage = "Error while connecting to db: ${e.message}"
      Logger.logError(
          "Ошибка при подключении к ClickHouse", errorMessage, stackTrace = e.stackTraceToString())
      throw RuntimeException(errorMessage, e)
    }
  }

  /**
   * Возвращает соединение из пула.
   *
   * Если пул пуст и общее число соединений меньше [MAX_POOL_SIZE], создается новое соединение.
   * Иначе ожидание соединения до 30 секунд.
   *
   * @return Соединение, обернутое в [PooledConnection].
   * @throws RuntimeException если время ожидания истекло.
   */
  fun getConnection(): Connection {
    Logger.logActivity("Запрос соединения из пула")

    val conn =
        connectionPool.poll()
            ?: run {
              if (totalConnections.get() < MAX_POOL_SIZE) {
                Logger.logActivity("Создание нового соединения (пул пуст)")
                totalConnections.incrementAndGet()
                createNewConnection()
              } else {
                Logger.logActivity(
                    "Ожидание освобождения соединения (пул исчерпан)", level = LogLevel.WARN)
                val connection = connectionPool.poll(30, TimeUnit.SECONDS)
                if (connection == null) {
                  val errorMessage = "Timeout waiting for a database connection"
                  Logger.logError(
                      "Таймаут ожидания соединения", errorMessage, level = LogLevel.ERROR)
                  throw RuntimeException(errorMessage)
                }
                connection
              }
            }

    Logger.logActivity("Соединение получено из пула")
    return PooledConnection(conn)
  }

  /**
   * Возвращает соединение в пул.
   *
   * @param connection Соединение для возврата.
   */
  internal fun releaseConnection(connection: Connection) {
    Logger.logActivity("Возврат соединения в пул", level = LogLevel.DEBUG)
    connectionPool.offer(connection)
  }

  /** Закрывает все соединения в пуле. */
  fun close() {
    Logger.logActivity("Закрытие всех соединений в пуле")

    var successCount = 0
    var errorCount = 0

    connectionPool.forEach { conn ->
      try {
        conn.close()
        successCount++
      } catch (e: SQLException) {
        errorCount++
        Logger.logError(
            "Ошибка при закрытии соединения",
            e.message ?: "Неизвестная ошибка",
            stackTrace = e.stackTraceToString())
        e.printStackTrace()
      }
    }

    Logger.logActivity("Закрытие соединений завершено")

    // Закрываем логгер в конце работы
    Logger.close()
  }

  /**
   * Обертка над соединением, возвращающая его в пул при вызове [close].
   *
   * @param connection Исходное соединение.
   */
  private class PooledConnection(private val connection: Connection) : Connection by connection {
    override fun close() {
      releaseConnection(connection)
    }
  }
}
