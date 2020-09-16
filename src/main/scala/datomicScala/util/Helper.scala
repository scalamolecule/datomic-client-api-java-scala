package datomicScala.util

import java.util.stream.{StreamSupport, Stream => jStream}
import java.util.{Spliterator, Spliterators, Iterator => jIterator}
import clojure.lang.{ILookup, LazySeq}
import datomic.Util.read
import datomic.core.db.Datum
import datomicScala.client.api.Datom
import datomicClojure.ClojureBridge


object Helper extends ClojureBridge{


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
}
