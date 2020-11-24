package datomicScala.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{Date, List => jList, Map => jMap}
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke, Lookup}
import datomicScala.client.api.{Datom, DbStats}
import datomicScala.{AnomalyWrapper, Helper}


case class Db(datomicDb: AnyRef) extends Lookup(datomicDb) with AnomalyWrapper {


  def dbStats: DbStats = {
    Helper.dbStats(
      isDevLocal,
      Invoke.dbStats(datomicDb).asInstanceOf[jMap[_, _]]
    )
  }


  // Time filters --------------------------------------

  def asOf(t: Long): Db = Db(Invoke.asOf(datomicDb, t))
  def asOf(d: Date): Db = Db(Invoke.asOf(datomicDb, d))

  def since(t: Long): Db = Db(Invoke.since(datomicDb, t))
  def since(d: Date): Db = Db(Invoke.since(datomicDb, d))


  // Presuming a `withDb` is passed.
  def `with`(withDb: AnyRef, stmts: jList[_]): TxReport =
    TxReport(Invoke.`with`(withDb, stmts).asInstanceOf[jMap[_, _]])

  // Convenience method to pass with-modified Db
  def `with`(db: Db, stmts: jList[_]): TxReport =
    `with`(db.datomicDb, stmts)

  // Convenience method to pass with-modified Db from TxReport
  def `with`(txReport: TxReport, stmts: jList[_]): TxReport =
    `with`(txReport.dbAfter.datomicDb, stmts)


  def history: Db = Db(Invoke.history(datomicDb))


  // Indexes --------------------------------------

  /**
   *
   * @param index      String :eavt, :aevt, :avet, or :vaet
   * @param components Optional vector in the same order as the index
   *                   containing one or more values to further narrow the
   *                   result.
   * @return List[datomicFacade.client.api.Datom] Wrapped Datoms with a unified api
   */
  def datoms(index: String, components: jList[_]): jStream[Datom] = {
    Helper.streamOfDatoms(
      Invoke.datoms(datomicDb, index, components)
    )
  }


  def indexRange(
    attrId: String,
    startValue: Option[Any] = None,
    endValue: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jStream[Datom] = {
    Helper.streamOfDatoms(
      Invoke.indexRange(
        datomicDb, attrId, startValue, endValue, timeout, offset, limit
      )
    )
  }


  // Pull --------------------------------------

  def pull(
    selector: String,
    eid: Any,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jMap[_, _] = {
    Invoke.pull(
      datomicDb, selector, eid, timeout, offset, limit
    ).asInstanceOf[jMap[_, _]]
  }


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean = false,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jStream[_] = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException(ErrorMsg.indexPull)
    Invoke.indexPull(
      datomicDb, index, selector, start, reverse, timeout, offset, limit
    ).asInstanceOf[clojure.lang.ASeq].stream()
  }
}