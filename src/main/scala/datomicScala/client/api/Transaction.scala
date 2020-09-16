package datomicScala.client.api

case class Transaction(
  t: Long,
  data: Iterable[Datom]
)
