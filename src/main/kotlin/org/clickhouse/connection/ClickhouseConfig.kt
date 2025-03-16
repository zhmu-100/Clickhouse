package org.clickhouse.connection

data class ClickhouseConfig(
    val clickhouseUrl: String,
    val clickhouseUser: String,
    val clickhousePassword: String,

    val apiHost: String,
    val apiPort: Int,
) {
    companion object{
        fun load(): ClickhouseConfig {
            val clickhouseUrl = EnvLoader.get("CLICKHOUSE_URL") ?: "jdbc:clickhouse://localhost:8123/default"
            val clickhouseUser = EnvLoader.get("CLICKHOUSE_USER") ?: "default"
            val clickhousePassword = EnvLoader.get("CLICKHOUSE_PASSWORD") ?: ""

            val apiHost = EnvLoader.get("API_HOST") ?: "0.0.0.0"
            val apiPort = EnvLoader.get("API_PORT")?.toIntOrNull() ?: 8080

            return ClickhouseConfig(clickhouseUrl, clickhouseUser, clickhousePassword, apiHost, apiPort)
        }
    }
}