package datomicScala.client.api.sync

import java.util
import java.util.{Iterator => jIterator, List => jList}
import clojure.lang.{ILookup, PersistentArrayMap, PersistentVector}
import datomic.Util._
import datomic.core.db.Datum
import datomicClojure.Invoke
import datomicScala.anomaly.AnomalyWrapper
import datomicScala.client.api.Datom
import datomicScala.util.Helper.getDatom


case class Connection(datomicConn: AnyRef) extends AnomalyWrapper {

  lazy private val isDevLocal = db.datomicDb.isInstanceOf[datomic.core.db.Db]


  def db: Db = catchAnomaly {
    Db(Invoke.db(datomicConn))
  }


  def transact(stmtss: jList[_]): TxReport = catchAnomaly {
    if (stmtss.isEmpty)
      throw new IllegalArgumentException("No transaction statements passed.")
    TxReport(Invoke.transact(datomicConn, stmtss))
  }


  def sync(t: Long): Db = catchAnomaly {
    Db(Invoke.sync(datomicConn, t))
  }


  def withDb: AnyRef = catchAnomaly {
    // Special db value for `with` (or `widh`)
    Invoke.withDb(datomicConn)
  }

  // Convenience method for single invocation from connection
  def widh(stmtss: jList[_]): Db = catchAnomaly {
    val txReport = Invoke.`with`(withDb, stmtss)
    val dbAfter  = txReport.get(read(":db-after"))
    Db(dbAfter.asInstanceOf[AnyRef])
  }


  // Lazy evaluation on both levels - txs and datoms of each tx
  def txRange(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Iterable[(Long, Iterable[Datom])] = {
    val rawTxs0 = catchAnomaly {
      Invoke.txRange(datomicConn, start, end, timeout, offset, limit)
    }
    if (isDevLocal) {
      val rawTxs = rawTxs0.asInstanceOf[clojure.lang.LazySeq]
      // Iterable with txs
      new Iterable[(Long, Iterable[Datom])] {
        override def iterator: Iterator[(Long, Iterable[Datom])] = {
          new Iterator[(Long, Iterable[Datom])] {
            val it: jIterator[_] = rawTxs.iterator
            override def hasNext: Boolean = it.hasNext
            override def next(): (Long, Iterable[Datom]) = {
              val tx          = it.next().asInstanceOf[PersistentArrayMap]
              val t           = tx.get(read(":t")).asInstanceOf[Long]
              val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]

              // Iterable with Datoms of this tx
              val txDatoms = new Iterable[Datom] {
                override def iterator: Iterator[Datom] = {
                  new Iterator[Datom] {
                    private val it = rawTxDatoms.iterator()
                    override def hasNext: Boolean = it.hasNext
                    override def next(): Datom = {
                      getDatom(it.next.asInstanceOf[Datum])
                    }
                  }
                }
              }
              (t, txDatoms)
            }
          }
        }
      }
    } else {
      val rawTxs = rawTxs0.asInstanceOf[java.lang.Iterable[_]]
      // Iterable with txs
      new Iterable[(Long, Iterable[Datom])] {
        override def iterator: Iterator[(Long, Iterable[Datom])] = {
          new Iterator[(Long, Iterable[Datom])] {
            val it: jIterator[_] = rawTxs.iterator
            override def hasNext: Boolean = it.hasNext
            override def next(): (Long, Iterable[Datom]) = {
              val tx          = it.next().asInstanceOf[PersistentArrayMap]
              val t           = tx.get(read(":t")).asInstanceOf[Long]
              val rawTxDatoms = tx.get(read(":data")).asInstanceOf[PersistentVector]

              // Iterable with Datoms of this tx
              val txDatoms = new Iterable[Datom] {
                override def iterator: Iterator[Datom] = {
                  new Iterator[Datom] {
                    private val it2 = rawTxDatoms.iterator()
                    override def hasNext: Boolean = it2.hasNext
                    override def next(): Datom = {
                      getDatom(it2.next.asInstanceOf[ILookup])
                    }
                  }
                }
              }
              (t, txDatoms)
            }
          }
        }
      }
    }
  }

  // Populated Array
  def txRangeArray(
    start: Option[Long] = None,
    end: Option[Long] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): Array[(Long, Array[Datom])] = {
    val rawTxs0 = catchAnomaly {
      Invoke.txRange(datomicConn, start, end, timeout, offset, limit)
    }
    // Create multi-dimensional Array
    if (isDevLocal) {
      val raw = rawTxs0.asInstanceOf[clojure.lang.LazySeq]
      val txs = new Array[(Long, Array[Datom])](raw.size())
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
        txs(i) = (t, txDatoms)
        i += 1
      }
      txs

    } else {
      val raw = rawTxs0.asInstanceOf[java.lang.Iterable[_]]
      val txs = new util.ArrayList[(Long, Array[Datom])]()
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
        txs.add((t, txDatoms))
        i += 1
      }
      txs.toArray(new Array[(Long, Array[Datom])](i))
    }
  }
}
