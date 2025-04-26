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
 *
 * Метод [load] загружает параметры из env.
 */
data class ClickhouseConfig(
    val clickhouseUrl: String,
    val clickhouseUser: String,
    val clickhousePassword: String,
    val apiHost: String,
    val apiPort: Int,
) {
  companion object {

    fun load(): ClickhouseConfig {
      /**
       * Загружает конфигурацию из nev.
       *
       * Использует следующие переменные:
       * - CLICKHOUSE_URL (по умолчанию: "jdbc:clickhouse://localhost:8123/default")
       * - CLICKHOUSE_USER (по умолчанию: "default")
       * - CLICKHOUSE_PASSWORD (по умолчанию: пустая строка)
       * - API_HOST (по умолчанию: "0.0.0.0")
       * - API_PORT (по умолчанию: 8080)
       *
       * @return Экземпляр [ClickhouseConfig] с загруженными параметрами.
       */
      val dotenv = dotenv()

      val clickhouseUrl = dotenv["CLICKHOUSE_URL"] ?: "jdbc:clickhouse://localhost:8123/default"
      val clickhouseUser = dotenv["CLICKHOUSE_USER"] ?: "default"
      val clickhousePassword = dotenv["CLICKHOUSE_PASSWORD"] ?: ""

      val apiHost = dotenv["API_HOST"] ?: "0.0.0.0"
      val apiPort = dotenv["API_PORT"]?.toIntOrNull() ?: 8080

      return ClickhouseConfig(clickhouseUrl, clickhouseUser, clickhousePassword, apiHost, apiPort)
    }
  }
}
