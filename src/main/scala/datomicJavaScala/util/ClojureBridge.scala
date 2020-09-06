package datomicJavaScala.util

import java.util.stream.{StreamSupport, Stream => jStream}
import java.util.{Spliterator, Spliterators, Iterator => jIterator, List => jList, Map => jMap}
import clojure.java.api.Clojure
import clojure.lang.{IFn, ILookup, LazySeq, Keyword => clKeyword, Symbol => clSymbol}
import datomic.core.db.Datum
import datomic.db.DbId
import datomicJavaScala.client.api.Datom
import us.bpsm.edn.printer.Printer.Fn
import us.bpsm.edn.printer.{Printer, Printers}

trait ClojureBridge {

  def fn(ns: String, method: String): IFn = Clojure.`var`(ns, method)

  def read(s: String): AnyRef = Clojure.read(s)

  lazy val deref = fn("clojure.core", "deref")

  lazy val requireFn: IFn = fn("clojure.core", "require")

  def require(nss: String): AnyRef = requireFn.invoke(read(nss))

  def clientFn(name: String): IFn = fn("datomic.client.api", name)

  def printLn(s: AnyRef): Unit = fn("clojure.core", "println").invoke(s)


  // EDN -----------------------------------------------------------------

  // Printing edn with us.bpsm.edn
  // Adding recognition of clojure Keyword and Datomic DbId

  lazy val clKw : Fn[_] = new Printer.Fn[clKeyword]() {
    override def eval(kw: clKeyword, writer: Printer): Unit = {
      writer.softspace.append(kw.toString).softspace
    }
  }
  lazy val clSym: Fn[_] = new Printer.Fn[clSymbol]() {
    override def eval(sym: clSymbol, writer: Printer): Unit = {
      writer.softspace.append(sym.toString).softspace
    }
  }
  lazy val dbId : Fn[_] = new Printer.Fn[DbId]() {
    override def eval(dbid: DbId, writer: Printer): Unit = {
      if (dbid.idx.asInstanceOf[Long] > 0) {
        // Entity id
        // :club/member 1557968513546 :next/attr ...
        writer.printValue(dbid.idx)
      } else {
        // Temp id treated as text to be resolved in tx
        // :club/member"-1000001":next/attr ...
        // Skipping partition information since the client.api disregards it anyway.
        writer.printValue(dbid.idx.toString)
      }
    }
  }

  lazy val compact = Printers.defaultProtocolBuilder
    .put(classOf[clKeyword], clKw)
    .put(classOf[clSymbol], clSym)
    .put(classOf[DbId], dbId)
    .build

  lazy val pretty = Printers.prettyProtocolBuilder
    .put(classOf[clKeyword], clKw)
    .put(classOf[clSymbol], clSym)
    .put(classOf[DbId], dbId)
    .build


  def edn(stmtss: jList[_]): String = {
    Printers.printString(compact, stmtss)
  }
  def edn(stmts: jMap[_, _]): String = {
    Printers.printString(compact, stmts)
  }

  def ednPretty(stmtss: jList[_]): String = {
    Printers.printString(pretty, stmtss)
  }
  def ednPretty(stmts: jMap[_, _]): String = {
    Printers.printString(pretty, stmts)
  }


  // Helper methods -------------------------------------------------

  // Unify Datoms in single fast iteration
  protected def arrayOfDatoms(rawDatoms: Any): jStream[Datom] = {
    rawDatoms match {
      // Dev-local only
      case lazySeq: LazySeq => mkStream(
        new jIterator[Datom] {
          val it: jIterator[_] = lazySeq.iterator
          var d: Datum = null
          override def hasNext: Boolean = it.hasNext
          override def next(): Datom = {
            d = it.next.asInstanceOf[Datum]
            Datom(d.e, d.a, d.v, d.tx.asInstanceOf[Long], d.added())
          }
        }
      )

      // Peer Server + Dev-local
      case iterable: java.lang.Iterable[_] => mkStream(
        new jIterator[Datom] {
          val it: jIterator[_] = iterable.iterator
          var d: ILookup = null
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

  def mkStream(it: jIterator[Datom]): jStream[Datom] = StreamSupport.stream(
    Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED),
    false
  )


  // Debug methods ------------------------------

  def types(obj: Any): Unit = {
    println("-----")
    println(obj.getClass)
    println(obj.getClass.getSuperclass)
    obj.getClass.getInterfaces.map(_.toString).sorted foreach println
    println("------------")
  }

  def methods(obj: Any): Unit = {
    obj.getClass.getMethods.map(_.toString).sorted foreach println
    println("------------")
  }
}
