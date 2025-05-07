package org.clickhouse.connection

import io.github.cdimascio.dotenv.dotenv

/**
 * Конфигурация подключения к базе данных ClickHouse и настройка REST API.
 *
 * Содержит параметры:
 * - [clickhouseUrl]: URL для подключения к ClickHouse.
 * - [clickhouseUser]: Имя пользователя для подключения.
 * - [clickhousePassword]: Пароль для подключения.
 * - [apiHost]: Хост для запуска REST API.
 * - [apiPort]: Порт для запуска REST API.
 * - [redisHost]: Хост Redis для логгера.
 * - [redisPort]: Порт Redis для логгера.
 * - [redisPassword]: Пароль Redis для логгера.
 * - [redisActivityChannel]: Канал Redis для логов активности.
 * - [redisErrorChannel]: Канал Redis для логов ошибок.
 *
 * Метод [load] загружает параметры из env.
 */
data class ClickhouseConfig(
    val clickhouseUrl: String,
    val clickhouseUser: String,
    val clickhousePassword: String,
    val apiHost: String,
    val apiPort: Int,
    val redisHost: String,
    val redisPort: Int,
    val redisPassword: String,
    val redisActivityChannel: String,
    val redisErrorChannel: String
) {
  companion object {

    fun load(): ClickhouseConfig {
      /**
       * Загружает конфигурацию из env.
       *
       * Использует следующие переменные:
       * - CLICKHOUSE_URL (по умолчанию: "jdbc:clickhouse://localhost:8123/default")
       * - CLICKHOUSE_USER (по умолчанию: "default")
       * - CLICKHOUSE_PASSWORD (по умолчанию: пустая строка)
       * - API_HOST (по умолчанию: "0.0.0.0")
       * - API_PORT (по умолчанию: 8080)
       * - REDIS_HOST (по умолчанию: "localhost")
       * - REDIS_PORT (по умолчанию: 6379)
       * - REDIS_PASSWORD (по умолчанию: пустая строка)
       * - REDIS_ACTIVITY_CHANNEL (по умолчанию: "logger:activity")
       * - REDIS_ERROR_CHANNEL (по умолчанию: "logger:error")
       *
       * @return Экземпляр [ClickhouseConfig] с загруженными параметрами.
       */
      val dotenv = dotenv()

      val clickhouseUrl = dotenv["CLICKHOUSE_URL"] ?: "jdbc:clickhouse://localhost:8123/default"
      val clickhouseUser = dotenv["CLICKHOUSE_USER"] ?: "default"
      val clickhousePassword = dotenv["CLICKHOUSE_PASSWORD"] ?: "default"

      val apiHost = dotenv["API_HOST"] ?: "0.0.0.0"
      val apiPort = dotenv["API_PORT"]?.toIntOrNull() ?: 8091

      val redisHost = dotenv["REDIS_HOST"] ?: "localhost"
      val redisPort = dotenv["REDIS_PORT"]?.toIntOrNull() ?: 6379
      val redisPassword = dotenv["REDIS_PASSWORD"] ?: ""
      val redisActivityChannel = dotenv["REDIS_ACTIVITY_CHANNEL"] ?: "logger:activity"
      val redisErrorChannel = dotenv["REDIS_ERROR_CHANNEL"] ?: "logger:error"

      return ClickhouseConfig(
          clickhouseUrl,
          clickhouseUser,
          clickhousePassword,
          apiHost,
          apiPort,
          redisHost,
          redisPort,
          redisPassword,
          redisActivityChannel,
          redisErrorChannel)
    }
  }
}
