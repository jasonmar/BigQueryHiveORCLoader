package com.google.cloud.bqhiveloader

// for compatibility
object BQHiveLoader {
  def main(args: Array[String]): Unit =
    com.google.cloud.imf.BQHiveLoader.main(args)
}
