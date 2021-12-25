package datomicScala.client.api

import java.lang.{Iterable => jIterable}
import java.util
import java.util.stream.{StreamSupport, Stream => jStream}
import java.util.{Spliterator, Spliterators, Iterator => jIterator, List => jList, Map => jMap}
import clojure.lang._
import datomic.Util.read
//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._

object Helper {

  lazy val e     = read(":e")
  lazy val a     = read(":a")
  lazy val v     = read(":v")
  lazy val tx    = read(":tx")
  lazy val added = read(":added")

  def getDatom(d: ILookup): Datom = Datom(
    d.valAt(e).asInstanceOf[Long],
    d.valAt(a),
    d.valAt(v),
    d.valAt(tx).asInstanceOf[Long],
    d.valAt(added).asInstanceOf[Boolean]
  )

  // Unify Datoms in single fast iteration
  def streamOfDatoms(rawDatoms: Any): jStream[Datom] = {
    rawDatoms match {
      // Dev-local only
      // Getting Datom values without reflection
      case lazySeq: LazySeq => mkStream(
        new jIterator[Datom] {
          val it: jIterator[_] = lazySeq.iterator
          override def hasNext: Boolean = it.hasNext
          override def next(): Datom = getDatom(it.next.asInstanceOf[ILookup])
        }
      )

      // Peer Server + Dev-local
      // Gets Datom values with reflection through ILookup interface
      case iterable: java.lang.Iterable[_] => mkStream(
        new jIterator[Datom] {
          val it: jIterator[_] = iterable.iterator
          override def hasNext: Boolean = it.hasNext
          override def next(): Datom = getDatom(it.next.asInstanceOf[ILookup])
        }
      )

      case _ => jStream.empty()
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
      if (rawTxs0.asInstanceOf[jList[_]].isEmpty)
        return Iterable.empty[(Long, Iterable[Datom])]
      val rawTxs = rawTxs0.asInstanceOf[LazySeq]
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
                    override def next(): Datom = getDatom(it2.next.asInstanceOf[ILookup])
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
      if (!rawTxs.iterator().hasNext)
        return Iterable.empty[(Long, Iterable[Datom])]
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
          txDatoms(j) = getDatom(d0.asInstanceOf[ILookup])
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
