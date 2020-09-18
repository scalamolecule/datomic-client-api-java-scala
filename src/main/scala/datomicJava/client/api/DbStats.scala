package datomicJava.client.api

import java.util.{Map => jMap}


case class DbStats(
  datoms: Long,
  attrs: jMap[String, Long]
)

