package datomicScala.client.api


case class Tx(
  t: Long,
  data: Iterable[Datom]
)
