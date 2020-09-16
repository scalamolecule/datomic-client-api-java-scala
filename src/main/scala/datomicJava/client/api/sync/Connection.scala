package datomicJava.client.api.sync

import java.util
import java.util.{Iterator => jIterator, List => jList, Map => jMap}
import java.lang.{Iterable => jIterable}
import javafx.util.Pair
import clojure.lang.{ILookup, PersistentArrayMap, PersistentVector}
import datomic.Util._
import datomic.core.db.Datum
import datomicClojure.Invoke
import datomicJava.anomaly.AnomalyWrapper
import datomicJava.client.api.Datom
import datomicJava.util.Helper.{getDatom, _}


case class Connection(datomicConn: AnyRef) extends AnomalyWrapper {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[datomic.core.db.Db]


  def db: Db = Db(syncFn("db").invoke(datomicConn))


  def transact(stmts: jList[_]): TxReport = {
    if (stmts.isEmpty)
      throw new IllegalArgumentException("No transaction statements passed.")
    TxReport(
      syncFn("transact").invoke(
        datomicConn, read(s"{:tx-data ${edn(stmts)}}")
      ).asInstanceOf[jMap[_, _]]
    )
  }


  def sync(t: Long): Db = Db(syncFn("sync").invoke(datomicConn, t))


  def withDb: AnyRef = {
    // Special db value for `with` (or `widh`)
    syncFn("with-db").invoke(datomicConn)
  }

  // Convenience method for single invocation from connection
  def widh(list: jList[_]): Db = {
    val txReport = syncFn("with").invoke(
      withDb, read(s"{:tx-data ${edn(list)}}")
    ).asInstanceOf[jMap[_, _]]
    val dbAfter  = txReport.get(read(":db-after"))
    Db(dbAfter.asInstanceOf[AnyRef])
  }


  // Lazy evaluation on both levels - txs and datoms of each tx

  def txRange(): jIterable[Pair[Long, jIterable[Datom]]] = {
    txRange(0, 0, 0, 0, 1000)
  }

  def txRange(
    start: Long,
    end: Long,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jIterable[Pair[Long, jIterable[Datom]]] = {
    val rawTxs0 = catchAnomaly {
      val startOpt  : Option[Long] = if (start == 0) None else Some(start)
      val endOpt    : Option[Long] = if (end == 0) None else Some(end)
      val timeoutOpt: Option[Int]  = if (timeout == 0) None else Some(timeout)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    // Create multi-dimensional Array
    if (isDevLocal) {
      val rawTxs = rawTxs0.asInstanceOf[clojure.lang.LazySeq]
      // Iterable with txs
      new jIterable[Pair[Long, jIterable[Datom]]] {
        override def iterator: jIterator[Pair[Long, jIterable[Datom]]] = {
          new jIterator[Pair[Long, jIterable[Datom]]] {
            val it: jIterator[_] = rawTxs.iterator
            override def hasNext: Boolean = it.hasNext
            override def next(): Pair[Long, jIterable[Datom]] = {
              val tx          = it.next().asInstanceOf[PersistentArrayMap]
              val t           = tx.get(read(":t")).asInstanceOf[Long]
              val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]

              // Iterable with Datoms of this tx
              val txDatoms = new jIterable[Datom] {
                override def iterator: jIterator[Datom] = {
                  new jIterator[Datom] {
                    private val it = rawTxDatoms.iterator()
                    override def hasNext: Boolean = it.hasNext
                    override def next(): Datom = {
                      getDatom(it.next.asInstanceOf[Datum])
                    }
                  }
                }
              }
              new Pair(t, txDatoms)
            }
          }
        }
      }
    } else {
      val rawTxs = rawTxs0.asInstanceOf[java.lang.Iterable[_]]
      // Iterable with txs
      new jIterable[Pair[Long, jIterable[Datom]]] {
        override def iterator: jIterator[Pair[Long, jIterable[Datom]]] = {
          new jIterator[Pair[Long, jIterable[Datom]]] {
            val it: jIterator[_] = rawTxs.iterator
            override def hasNext: Boolean = it.hasNext
            override def next(): Pair[Long, jIterable[Datom]] = {
              val tx          = it.next().asInstanceOf[PersistentArrayMap]
              val t           = tx.get(read(":t")).asInstanceOf[Long]
              val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]

              // Iterable with Datoms of this tx
              val txDatoms = new jIterable[Datom] {
                override def iterator: jIterator[Datom] = {
                  new jIterator[Datom] {
                    private val it2 = rawTxDatoms.iterator()
                    override def hasNext: Boolean = it2.hasNext
                    override def next(): Datom = {
                      getDatom(it2.next.asInstanceOf[ILookup])
                    }
                  }
                }
              }
              new Pair(t, txDatoms)
            }
          }
        }
      }
    }
  }

  def txRangeArray(): Array[Pair[Long, Array[Datom]]] = {
    txRangeArray(0, 0, 0, 0, 1000)
  }

  def txRangeArray(
    start: Long,
    end: Long,
    timeout: Int,
    offset: Int,
    limit: Int
  ): Array[Pair[Long, Array[Datom]]] = {
    val rawTxs0 = catchAnomaly {
      val startOpt  : Option[Long] = if (start == 0) None else Some(start)
      val endOpt    : Option[Long] = if (end == 0) None else Some(end)
      Invoke.txRange(datomicConn, startOpt, endOpt, timeout, offset, limit)
    }
    // Create multi-dimensional Array
    if (isDevLocal) {
      val raw = rawTxs0.asInstanceOf[clojure.lang.LazySeq]
      val txs = new Array[Pair[Long, Array[Datom]]](raw.size())
      var i   = 0
      raw.forEach { tx0 =>
        val tx          = tx0.asInstanceOf[PersistentArrayMap]
        val t           = tx.get(read(":t")).asInstanceOf[Long]
        val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]
        val txDatoms    = new Array[Datom](rawTxDatoms.size())
        var j           = 0
        rawTxDatoms.forEach { d0 =>
          txDatoms(j) = getDatom(d0.asInstanceOf[Datum])
          j += 1
        }
        txs(i) = new Pair(t, txDatoms)
        i += 1
      }
      txs

    } else {

      val raw = rawTxs0.asInstanceOf[java.lang.Iterable[_]]
      val txs = new util.ArrayList[Pair[Long, Array[Datom]]]()
      var i   = 0
      raw.forEach { tx0 =>
        val tx          = tx0.asInstanceOf[PersistentArrayMap]
        val t           = tx.get(read(":t")).asInstanceOf[Long]
        val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]
        val txDatoms    = new Array[Datom](rawTxDatoms.size())
        var j           = 0
        rawTxDatoms.forEach { d0 =>
          txDatoms(j) = getDatom(d0.asInstanceOf[ILookup])
          j += 1
        }
        txs.add(new Pair(t, txDatoms))
        i += 1
      }
      txs.toArray(new Array[Pair[Long, Array[Datom]]](i))
    }
  }
}
