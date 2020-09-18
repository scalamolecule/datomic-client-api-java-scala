package datomicScala

import java.lang.{Iterable => jIterable}
import java.util
import java.util.stream.{StreamSupport, Stream => jStream}
import java.util.{Spliterator, Spliterators, Iterator => jIterator, Map => jMap}
import clojure.lang._
import datomic.Util.read
import datomic.core.db.Datum
import datomicScala.client.api.{Datom, DbStats}
import scala.jdk.CollectionConverters._


object Helper {

  def getDatom(d: Datum): Datom = Datom(
    d.e, d.a, d.v, d.tx.asInstanceOf[Long], d.added()
  )

  def getDatom(d: ILookup): Datom = Datom(
    d.valAt(read(":e")).asInstanceOf[Long],
    d.valAt(read(":a")),
    d.valAt(read(":v")),
    d.valAt(read(":tx")).asInstanceOf[Long],
    d.valAt(read(":added")).asInstanceOf[Boolean]
  )


  // Unify Datoms in single fast iteration
  def streamOfDatoms(rawDatoms: Any): jStream[Datom] = {
    rawDatoms match {
      // Dev-local only
      // Getting Datom values without reflection
      case lazySeq: LazySeq => mkStream(
        new jIterator[Datom] {
          val it: jIterator[_] = lazySeq.iterator
          var d : Datum        = null
          override def hasNext: Boolean = it.hasNext
          override def next(): Datom = {
            d = it.next.asInstanceOf[Datum]
            Datom(d.e, d.a, d.v, d.tx.asInstanceOf[Long], d.added())
          }
        }
      )

      // Peer Server + Dev-local
      // Gets Datom values with reflection through ILookup interface
      case iterable: java.lang.Iterable[_] => mkStream(
        new jIterator[Datom] {
          val it: jIterator[_] = iterable.iterator
          var d : ILookup      = null
          def valAt(d: ILookup, key: String): Any = d.valAt(read(key))
          override def hasNext: Boolean = it.hasNext
          override def next(): Datom = {
            d = it.next.asInstanceOf[ILookup]
            Datom(
              valAt(d, ":e").asInstanceOf[Long],
              valAt(d, ":a"),
              valAt(d, ":v"),
              valAt(d, ":tx").asInstanceOf[Long],
              valAt(d, ":added").asInstanceOf[Boolean]
            )
          }
        }
      )
    }
  }

  private def mkStream(it: jIterator[Datom]): jStream[Datom] = StreamSupport.stream(
    Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED),
    false
  )

  def nestedTxsIterable(
    isDevLocal: Boolean,
    rawTxs0: AnyRef
  ): Iterable[(Long, Iterable[Datom])] = {
    if (isDevLocal) {
      val rawTxs = rawTxs0.asInstanceOf[LazySeq]
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
      val rawTxs = rawTxs0.asInstanceOf[jIterable[_]]
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

  def nestedTxsArray(
    isDevLocal: Boolean,
    rawTxs0: AnyRef
  ): Array[(Long, Array[Datom])] = {

    // Create multi-dimensional Array
    if (isDevLocal) {
      val rawTxs = rawTxs0.asInstanceOf[clojure.lang.LazySeq]
      val txs    = new Array[(Long, Array[Datom])](rawTxs.size())
      var i      = 0
      rawTxs.forEach { tx0 =>
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
      val rawTxs = rawTxs0.asInstanceOf[java.lang.Iterable[_]]
      val txs    = new util.ArrayList[(Long, Array[Datom])]()
      var i      = 0
      rawTxs.forEach { tx0 =>
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


  def dbStats(isDevLocal: Boolean, raw: jMap[_, _]): DbStats = {
    val datoms   = raw.get(read(":datoms")).asInstanceOf[Long]
    val attrsRaw = raw.get(read(":attrs"))
    val count    = if (isDevLocal)
      (rawCount: Any) => rawCount.asInstanceOf[Int].toLong
    else
      (rawCount: Any) => rawCount.asInstanceOf[Long]
    val attrs    = if (attrsRaw != null)
      Some(
        attrsRaw.asInstanceOf[jMap[_, _]].asScala.toMap.map {
          case (kw: Keyword, m: jMap[_, _]) =>
            kw.toString -> count(m.asScala.head._2)

          case otherPair => throw new RuntimeException(
            "Unexpected pair from db-stats: " + otherPair
          )
        }
      ) else None
    DbStats(datoms, attrs)
  }
}
