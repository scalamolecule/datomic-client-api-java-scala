package datomicScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{Date, Map => jMap}
import datomic.Util._
import datomicScala.client.api.Helper._
import datomicScala.client.api.Datom

/** Facade to Datomic transaction report */
case class AsyncTxReport(rawTxReport: jMap[_, _]) {

  /** Get database value before transaction. */
  lazy val dbBefore: AsyncDb = AsyncDb(rawTxReport.get(read(":db-before")).asInstanceOf[AnyRef])

  /** Get database value after transaction. */
  lazy val dbAfter: AsyncDb = AsyncDb(rawTxReport.get(read(":db-after")).asInstanceOf[AnyRef])

  /** Get Array of transacted Datoms. */
  def txData: jStream[Datom] = streamOfDatoms(rawTxReport.get(read(":tx-data")))

  /** Get map of temp ids and entity ids. */
  lazy val tempIds: jMap[Long, Long] =
    rawTxReport.get(read(":tempids")).asInstanceOf[jMap[Long, Long]]

  // Convenience accessors
  lazy val txDatom: Datom = txData.iterator().next()
  lazy val basisT : Long  = dbBefore.t
  lazy val t      : Long  = dbAfter.t
  lazy val tx     : Long  = txDatom.e
  lazy val txInst : Date  = txDatom.v.asInstanceOf[Date]
}

