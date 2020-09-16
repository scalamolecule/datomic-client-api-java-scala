package datomicJava.client.api

import java.util.{Map => jMap}


/* todo: other possible stats? */
class DbStats(
  val datoms: Long,
  val attrs: jMap[String, Long]
)

