package datomicScala.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
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

  def since(t: Long): Db = Db(Invoke.since(datomicDb, t))

  def `with`(withDb: AnyRef, stmts: jList[_]): Db = {
    if (withDb.isInstanceOf[Db])
      throw new IllegalArgumentException(ErrorMsg.`with`)
    Db(
      Invoke.`with`(withDb, stmts).asInstanceOf[jMap[_, _]]
        .get(read(":db-after")).asInstanceOf[AnyRef]
    )
  }

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
    start: Option[Any] = None,
    end: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jStream[Datom] = {
    Helper.streamOfDatoms(
      Invoke.indexRange(
        datomicDb, attrId, start, end, timeout, offset, limit
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