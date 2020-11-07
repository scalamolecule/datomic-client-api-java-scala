package datomicJava.client.api

import java.lang.{Iterable => jIterable}


case class Tx(
  t: Long,
  data: jIterable[Datom]
)
