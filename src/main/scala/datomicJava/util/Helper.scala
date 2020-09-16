package datomicJava.util

import java.util.stream.{StreamSupport, Stream => jStream}
import java.util.{Spliterator, Spliterators, Iterator => jIterator}
import clojure.lang.{ILookup, LazySeq}
import datomic.Util.read
import datomic.core.db.Datum
import datomicJava.client.api.Datom
import datomicClojure.ClojureBridge

object Helper extends ClojureBridge {


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
          override def hasNext: Boolean = it.hasNext
          override def next(): Datom = getDatom(it.next.asInstanceOf[Datum])
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
    }
  }

  private def mkStream(it: jIterator[Datom]): jStream[Datom] = StreamSupport.stream(
    Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED),
    false
  )
}
