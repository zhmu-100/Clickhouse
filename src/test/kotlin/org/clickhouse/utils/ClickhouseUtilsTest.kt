package org.clickhouse.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ClickhouseUtilsTest {

  @Test
  fun `validateIdentifier should throw on invalid identifiers`() {
    assertThrows(RuntimeException::class.java) {
      ClickhouseUtils.validateIdentifier("invalid-name")
    }
  }

  @Test
  fun `substitutePlaceholders should throw if not enough params`() {
    assertThrows(RuntimeException::class.java) {
      ClickhouseUtils.substitutePlaceholders("id = ? AND name = ?", listOf(1))
    }
  }
}
