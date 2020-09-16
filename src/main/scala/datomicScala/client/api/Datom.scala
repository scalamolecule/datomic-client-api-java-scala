package datomicScala.client.api

case class Datom(
  e: Long,
  a: Any, // Keyword or Long
  v: Any, // Any value type
  tx: Long,
  added: Boolean
)