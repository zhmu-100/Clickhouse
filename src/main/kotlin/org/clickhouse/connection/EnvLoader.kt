package org.clickhouse.connection

import java.io.File

object EnvLoader {
  private val envMap: Map<String, String> = loadEnv()

  private fun loadEnv(): Map<String, String> {
    val map = mutableMapOf<String, String>()
    val file = File(".env")
    if (file.exists()) {
      file.forEachLine { line ->
        val trimmedLine = line.trim()
        if (trimmedLine.isEmpty() || trimmedLine.startsWith("#")) return@forEachLine

        val parts = trimmedLine.split("=")
        if (parts.size >= 2) {
          val key = parts[0].trim()
          val value = parts.subList(1, parts.size).joinToString("=").trim()
          map[key] = value
        }
      }
    }
    return map
  }

  fun get(key: String): String? {
    return System.getenv(key) ?: envMap[key]
  }
}
