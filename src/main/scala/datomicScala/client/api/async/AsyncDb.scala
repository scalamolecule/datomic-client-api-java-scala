package datomicScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.ASeq
import datomic.Util._
import datomicClojure.{ErrorMsg, Invoke, InvokeAsync, Lookup}
import datomicScala.Helper
import datomicScala.client.api.{Datom, DbStats}


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
          InvokeAsync.`with`(withDb, stmts)
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
      Helper.streamOfDatoms(
        Channel[Any](
          InvokeAsync.datoms(datomicDb, index, components)
        ).realize
      )
    )
  }


  def indexRange(
    attrId: String,
    start: Option[Any] = None,
    end: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Channel[jStream[Datom]] = {
    Channel[jStream[Datom]](
      Helper.streamOfDatoms(
        Channel[Any](
          InvokeAsync.indexRange(
            datomicDb, attrId, start, end, timeout, offset, limit
          )
        ).realize
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
  ): Channel[jMap[_, _]] = {
    Channel[jMap[_, _]](
      InvokeAsync.pull(
        datomicDb, selector, eid, timeout, offset, limit
      )
    )
  }


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean = false,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Channel[jStream[_]] = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException(ErrorMsg.indexPull)
    Channel[jStream[_]](
      Channel[Any](
        Invoke.indexPull(
          datomicDb, index, selector, start, reverse, timeout, offset, limit
        )
      ).realize.asInstanceOf[ASeq].stream()
    )
  }
}