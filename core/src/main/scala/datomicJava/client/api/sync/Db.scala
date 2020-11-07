package datomicJava.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke, Lookup}
import datomicJava.Helper
import datomicJava.Helper.streamOfDatoms
import datomicJava.client.api.{Datom, DbStats}


case class Db(datomicDb: AnyRef) extends Lookup(datomicDb) {


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
    streamOfDatoms(
      Invoke.datoms(datomicDb, index, components)
    )
  }


  def indexRange(
    attrId: String,
    start: Any,
    end: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jStream[Datom] = {
    streamOfDatoms(
      Invoke.indexRange(
        datomicDb, attrId, Option(start), Option(end), timeout, offset, limit
      )
    )
  }

  def indexRange[T](attrId: String): jStream[Datom] =
    indexRange(attrId, None, None, 0, 0, 1000)

  def indexRange[T](attrId: String, start: Option[Any]): jStream[Datom] =
    indexRange(attrId, start, None, 0, 0, 1000)

  def indexRange[T](
    attrId: String,
    start: Any,
    end: Any
  ): jStream[Datom] = indexRange(attrId, start, end, 0, 0, 1000)


  // Pull --------------------------------------

  def pull(
    selector: String,
    eid: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jMap[_, _] = {
    Invoke.pull(
      datomicDb, selector, eid, timeout, offset, limit
    ).asInstanceOf[jMap[_, _]]
  }

  def pull(selector: String, eid: Any): jMap[_, _] =
    pull(selector, eid, 0, 0, 1000)

  def pull(selector: String, eid: Any, timeout: Int): jMap[_, _] =
    pull(selector, eid, timeout, 0, 1000)


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    timeout: Int,
    offset: Int,
    limit: Int,
  ): jStream[_] = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException(ErrorMsg.indexPull)
    Invoke.indexPull(
      datomicDb, index, selector, start, reverse, timeout, offset, limit
    ).asInstanceOf[clojure.lang.ASeq].stream()
  }

  def indexPull(
    index: String,
    selector: String,
    start: String
  ): jStream[_] = {
    indexPull(index, selector, start, false, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean
  ): jStream[_] = {
    indexPull(index, selector, start, reverse, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    limit: Int,
  ): jStream[_] = {
    indexPull(index, selector, start, reverse, 0, 0, limit)
  }
}