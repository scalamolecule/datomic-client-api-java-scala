package datomicJavaScala.client.api

import java.util
import java.util.{List => jList, Map => jMap}
import clojure.lang.{ILookup, PersistentArrayMap, PersistentVector}
import datomic.Util
import datomic.core.db.Datum
import datomicJavaScala.util.ClojureBridge


case class Connection(datomicConn: AnyRef) extends ClojureBridge {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[datomic.core.db.Db]


  def db: Db = Db(clientFn("db").invoke(datomicConn))


  def transact(stmts: jList[_]): TxReport = {
    if (stmts.isEmpty)
      throw new IllegalArgumentException("No transaction statements passed.")
    TxReport(
      clientFn("transact").invoke(
        datomicConn, read(s"{:tx-data ${edn(stmts)}}")
      ).asInstanceOf[jMap[_, _]]
    )
  }


  def sync(t: Long): Db = Db(clientFn("sync").invoke(datomicConn, t))


  def withDb: AnyRef = {
    // Special db value for `with` (or `widh`)
    clientFn("with-db").invoke(datomicConn)
  }

  // Convenience method for single invocation from connection
  def widh(list: jList[_]): Db = {
    val txReport = clientFn("with").invoke(
      withDb, read(s"{:tx-data ${edn(list)}}")
    ).asInstanceOf[jMap[_, _]]
    val dbAfter  = txReport.get(Util.read(":db-after"))
    Db(dbAfter.asInstanceOf[AnyRef])
  }


  def txRange(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Option[Int] = None,
    offset: Int = 0,
    limit: Int = 1000
  ): Array[(Long, Array[Datom])] = {
    val start_   = start.fold("")(s => s":start $s")
    val end_     = end.fold("")(e => s":end $e")
    val timeout_ = timeout.fold("")(t => s":timeout $t")
    val offset_  = if (offset == 0) "" else s":offset $offset"
    val limit_   = if (limit == 1000) "" else s":limit $limit"
    val raw0     = clientFn("tx-range").invoke(
      datomicConn,
      read(
        s"""{
           |$start_
           |$end_
           |$timeout_
           |$offset_
           |$limit_
           |}""".stripMargin
      )
    )
    // Create multi-dimensional Array
    var i        = 0
    if (isDevLocal) {
      val raw = raw0.asInstanceOf[clojure.lang.LazySeq]
      val txs = new Array[(Long, Array[Datom])](raw.size())
      raw.forEach { tx0 =>
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
      txs

    } else {

      val raw = raw0.asInstanceOf[java.lang.Iterable[_]]
      val txs = new util.ArrayList[(Long, Array[Datom])]()
      def valAt(d: ILookup, key: String): Any = d.valAt(read(key))
      raw.forEach { tx0 =>
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
    }
  }
}
