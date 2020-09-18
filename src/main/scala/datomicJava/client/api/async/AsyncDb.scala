package datomicJava.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.LazySeq
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke, InvokeAsync, Lookup}
import datomicJava.Helper
import datomicJava.Helper._
import datomicJava.client.api.{Datom, DbStats}


case class AsyncDb(datomicDb: AnyRef) extends Lookup(datomicDb) {

  def dbStats: Channel[DbStats] = {
    Channel[DbStats](
      Helper.dbStats(
        isDevLocal,
        Channel[jMap[_, _]](
          InvokeAsync.dbStats(datomicDb)
        ).realize
      )
    )
  }


  // Time filters --------------------------------------

  def asOf(t: Long): AsyncDb =
    AsyncDb(
      InvokeAsync.asOf(datomicDb, t)
    )

  def since(t: Long): AsyncDb =
    AsyncDb(
      InvokeAsync.since(datomicDb, t)
    )

  def `with`(withDb: AnyRef, stmts: jList[_]): Channel[AsyncDb] = {
    if (withDb.isInstanceOf[AsyncDb])
      throw new IllegalArgumentException(ErrorMsg.`with`)
    Channel[AsyncDb](
      AsyncDb(
        Channel[AnyRef](
          InvokeAsync.`with`(withDb, stmts).asInstanceOf[jMap[_, _]]
            .get(read(":db-after")).asInstanceOf[AnyRef]
        ).realize.asInstanceOf[jMap[_, _]]
          .get(read(":db-after")).asInstanceOf[AnyRef]
      )
    )
  }

  def history: AsyncDb = AsyncDb(
    InvokeAsync.history(datomicDb)
  )


  // Indexes --------------------------------------

  /**
   *
   * @param index      String :eavt, :aevt, :avet, or :vaet
   * @param components Optional vector in the same order as the index
   *                   containing one or more values to further narrow the
   *                   result.
   * @return List[datomicFacade.client.api.Datom] Wrapped Datoms with a unified api
   */
  def datoms(index: String, components: jList[_]): Channel[jStream[Datom]] = {
    Channel[jStream[Datom]](
      streamOfDatoms(
        Channel[Any](
          InvokeAsync.datoms(datomicDb, index, components)
        ).realize
      )
    )
  }

  def indexRange[T](
    attrId: String,
    start: Any,
    end: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Channel[jStream[Datom]] = {
    Channel[jStream[Datom]](
      streamOfDatoms(
        Channel[Any](
          InvokeAsync.indexRange(
            datomicDb, attrId, Option(start), Option(end), timeout, offset, limit
          )
        ).realize
      )
    )
  }

  def indexRange[T](attrId: String): Channel[jStream[Datom]] =
    indexRange(attrId, None, None, 0, 0, 1000)

  def indexRange[T](attrId: String, start: Option[Any]): Channel[jStream[Datom]] =
    indexRange(attrId, start, None, 0, 0, 1000)

  def indexRange[T](
    attrId: String,
    start: Any,
    end: Any
  ): Channel[jStream[Datom]] = indexRange(attrId, start, end, 0, 0, 1000)


  // Pull --------------------------------------

  def pull(
    selector: String,
    eid: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Channel[jMap[_, _]] = {
    Channel[jMap[_, _]](
      InvokeAsync.pull(datomicDb, selector, eid, timeout, offset, limit)
    )
  }

  def pull(selector: String, eid: Any): Channel[jMap[_, _]] =
    pull(selector, eid, 0, 0, 1000)

  def pull(selector: String, eid: Any, limit: Int): Channel[jMap[_, _]] =
    pull(selector, eid, 0, 0, limit)


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    timeout: Int,
    offset: Int,
    limit: Int,
  ): Channel[jStream[_]] = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException(ErrorMsg.indexPull)
    Channel[jStream[_]](
      Channel[Any](
        Invoke.indexPull(
          datomicDb, index, selector, start, reverse, timeout, offset, limit
        )
      ).realize.asInstanceOf[LazySeq].stream()
    )
  }

  def indexPull(
    index: String,
    selector: String,
    start: String
  ): Channel[jStream[_]] = {
    indexPull(index, selector, start, false, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean
  ): Channel[jStream[_]] = {
    indexPull(index, selector, start, reverse, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    limit: Int,
  ): Channel[jStream[_]] = {
    indexPull(index, selector, start, reverse, 0, 0, limit)
  }
}