package org.clickhouse.connection

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

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
    repeat(INITIAL_POOL_SIZE) {
      val conn = createNewConnection()
      connectionPool.offer(conn)
      totalConnections.incrementAndGet()
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
    return try {
      DriverManager.getConnection(url, config.clickhouseUser, config.clickhousePassword)
    } catch (e: SQLException) {
      throw RuntimeException("Error while connecting to db: ${e.message}", e)
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
    val conn =
        connectionPool.poll()
            ?: run {
              if (totalConnections.get() < MAX_POOL_SIZE) {
                totalConnections.incrementAndGet()
                createNewConnection()
              } else {
                connectionPool.poll(30, TimeUnit.SECONDS)
                    ?: throw RuntimeException("Timeout waiting for a database connection")
              }
            }
    return PooledConnection(conn)
  }

  /**
   * Возвращает соединение в пул.
   *
   * @param connection Соединение для возврата.
   */
  internal fun releaseConnection(connection: Connection) {
    connectionPool.offer(connection)
  }

  /** Закрывает все соединения в пуле. */
  fun close() {
    connectionPool.forEach { conn ->
      try {
        conn.close()
      } catch (e: SQLException) {
        e.printStackTrace()
      }
    }
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
