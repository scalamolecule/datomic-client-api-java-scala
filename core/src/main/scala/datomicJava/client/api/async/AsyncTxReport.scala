package datomicJava.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{Map => jMap}
import datomic.Util._
import datomicJava.client.api.Datom
import datomicJava.Helper._

/** Facade to Datomic transaction report */
case class AsyncTxReport(rawTxReport: jMap[_, _]) {

  /** Get database value before transaction. */
  def dbBefore: AsyncDb = AsyncDb(rawTxReport.get(read(":db-before")).asInstanceOf[AnyRef])

  /** Get database value after transaction. */
  def dbAfter: AsyncDb = AsyncDb(rawTxReport.get(read(":db-after")).asInstanceOf[AnyRef])

  /** Get Array of transacted Datoms. */
  def txData: jStream[Datom] = streamOfDatoms(rawTxReport.get(read(":tx-data")))

  /** Get map of temp ids and entity ids. */
  def tempIds: jMap[Long, Long] =
    rawTxReport.get(read(":tempids")).asInstanceOf[jMap[Long, Long]]
}

