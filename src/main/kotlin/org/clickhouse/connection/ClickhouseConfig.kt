package org.clickhouse.connection

import io.github.cdimascio.dotenv.dotenv

data class ClickhouseConfig(
    val clickhouseUrl: String,
    val clickhouseUser: String,
    val clickhousePassword: String,
    val apiHost: String,
    val apiPort: Int,
) {
  companion object {
    fun load(): ClickhouseConfig {
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
