package org.clickhouse.connection

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

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

  private fun createNewConnection(): Connection {
    val config = ClickhouseConfig.load()
    val url = config.clickhouseUrl
    return try {
      DriverManager.getConnection(url, config.clickhouseUser, config.clickhousePassword)
    } catch (e: SQLException) {
      throw RuntimeException("Error while connecting to db: ${e.message}", e)
    }
  }

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

  internal fun releaseConnection(connection: Connection) {
    connectionPool.offer(connection)
  }

  fun close() {
    connectionPool.forEach { conn ->
      try {
        conn.close()
      } catch (e: SQLException) {
        e.printStackTrace()
      }
    }
  }

  private class PooledConnection(private val connection: Connection) : Connection by connection {
    override fun close() {
      releaseConnection(connection)
    }
  }
}
