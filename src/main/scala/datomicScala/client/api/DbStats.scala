package datomicScala.client.api


case class DbStats(
  datoms: Long,
  attrs: Option[Map[String, Long]]
)

