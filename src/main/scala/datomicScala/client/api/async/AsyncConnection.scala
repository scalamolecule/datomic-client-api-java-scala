package datomicScala.client.api.async

import java.util
import java.util.{List => jList, Map => jMap}
import clojure.lang.{ILookup, PersistentArrayMap, PersistentVector}
import datomic.Util._
import datomic.core.db.Datum
import datomicScala.client.api.Datom
import datomicScala.util.Helper._


case class AsyncConnection(connChannel: Channel[AnyRef]) {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[datomic.core.db.Db]


  def db: AsyncDb = {
    val conn = connChannel.realize
    AsyncDb(datomicAsyncFn("db").invoke(conn))
  }


  def transact(stmts: jList[_]): Channel[AsyncTxReport] = {
    if (stmts.isEmpty)
      throw new IllegalArgumentException("No transaction statements passed.")

    new Channel[AsyncTxReport](
      AsyncTxReport(
        new Channel[jMap[_, _]](
          datomicAsyncFn("transact").invoke(
            connChannel.realize, read(s"{:tx-data ${edn(stmts)}}")
          )
        ).realize
      )
    )
  }


  def sync(t: Long): Channel[AsyncDb] = new Channel[AsyncDb](
    AsyncDb(
      new Channel[datomic.core.db.Db](
        datomicAsyncFn("sync").invoke(connChannel.realize, t)
      ).realize
    )
  )


  def withDb: Channel[AnyRef] = new Channel[AnyRef](
    // Special db value for `with` (or `widh`)
    datomicAsyncFn("with-db").invoke(connChannel.realize)
  )

  // Convenience method for single invocation from connection
  def widh(list: jList[_]): AsyncDb = {
    val txReport = new Channel[AnyRef](
      datomicAsyncFn("with").invoke(
        withDb.realize, read(s"{:tx-data ${edn(list)}}")
      )
    ).realize.asInstanceOf[jMap[_, _]]
    val dbAfter  = txReport.get(read(":db-after"))
    AsyncDb(dbAfter.asInstanceOf[AnyRef])
  }


  def txRange(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Option[Int] = None,
    offset: Int = 0,
    limit: Int = 1000
  ): Channel[Array[(Long, Array[Datom])]] = {
    val start_   = start.fold("")(s => s":start $s")
    val end_     = end.fold("")(e => s":end $e")
    val timeout_ = timeout.fold("")(t => s":timeout $t")
    val offset_  = if (offset == 0) "" else s":offset $offset"
    val limit_   = if (limit == 1000) "" else s":limit $limit"
    val raw0     = new Channel[AnyRef](
      datomicAsyncFn("tx-range").invoke(
        connChannel.realize,
        read(
          s"""{
             |$start_
             |$end_
             |$timeout_
             |$offset_
             |$limit_
             |}""".stripMargin
        )
      ),
      // Some conversion needed?
      // Some((res: AnyRef) => res.asInstanceOf[LazySeq].stream)
    ).realize

    // todo: why do we only get 1 transaction / PersistentArrayMap back when
    //  calling <!! on channel?
    // Wrapping in a list for now to maintain logic and until we find a way to
    // return multiple transactions
    val raw = List(raw0)

    // Create multi-dimensional Array
    var i = 0
    if (isDevLocal) {
      //      val raw = raw0.asInstanceOf[clojure.lang.LazySeq]
      val txs = new Array[(Long, Array[Datom])](raw.size)
      raw.foreach { tx0 =>
        val tx          = tx0.asInstanceOf[PersistentArrayMap]
        val t           = tx.get(read(":t")).asInstanceOf[Long]
        val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]
        val txDatoms    = new Array[Datom](rawTxDatoms.size())
        var d: Datum    = null // Note datomic.core.db.Datum, not Datom
        var j           = 0
        rawTxDatoms.forEach { d0 =>
          d = d0.asInstanceOf[Datum]
          txDatoms(j) = Datom(d.e, d.a, d.v, d.tx.asInstanceOf[Long], d.added())
          j += 1
        }
        txs(i) = (t, txDatoms)
        i += 1
      }
      new Channel[Array[(Long, Array[Datom])]](txs)

    } else {

      //      val raw = raw0.asInstanceOf[java.lang.Iterable[_]]
      val txs = new util.ArrayList[(Long, Array[Datom])]()
      def valAt(d: ILookup, key: String): Any = d.valAt(read(key))
      raw.foreach { tx0 =>
        val tx          = tx0.asInstanceOf[PersistentArrayMap]
        val t           = tx.get(read(":t")).asInstanceOf[Long]
        val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]
        val txDatoms    = new Array[Datom](rawTxDatoms.size())
        var d: ILookup  = null
        var j           = 0
        rawTxDatoms.forEach { d0 =>
          d = d0.asInstanceOf[ILookup]
          txDatoms(j) = Datom(
            valAt(d, ":e").asInstanceOf[Long],
            valAt(d, ":a"),
            valAt(d, ":v"),
            valAt(d, ":tx").asInstanceOf[Long],
            valAt(d, ":added").asInstanceOf[Boolean]
          )
          j += 1
        }
        txs.add((t, txDatoms))
        i += 1
      }
      txs.toArray(new Array[(Long, Array[Datom])](i))

      new Channel[Array[(Long, Array[Datom])]](
        txs.toArray(new Array[(Long, Array[Datom])](i))
      )
    }
  }
}
