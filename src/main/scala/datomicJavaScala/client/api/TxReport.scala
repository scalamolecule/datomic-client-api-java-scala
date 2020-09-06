package datomicJavaScala.client.api

import java.util.stream.{Stream => jStream}
import java.util.{Map => jMap}
import datomicJavaScala.util.ClojureBridge

/** Facade to Datomic transaction report */
case class TxReport(rawTxReport: jMap[_, _]) extends ClojureBridge {

  /** Get database value before transaction. */
  def dbBefore: Db = Db(rawTxReport.get(read(":db-before")).asInstanceOf[AnyRef])

  /** Get database value after transaction. */
  def dbAfter: Db = Db(rawTxReport.get(read(":db-after")).asInstanceOf[AnyRef])

  /** Get Array of transacted Datoms. */
  def txData: jStream[Datom] = arrayOfDatoms(rawTxReport.get(read(":tx-data")))

  /** Get map of temp ids and entity ids. */
  def tempIds: jMap[Long, Long] =
    rawTxReport.get(read(":tempids")).asInstanceOf[jMap[Long, Long]]
}

