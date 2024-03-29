package datomicClient

import java.net.URI
import java.util.{List => jList, Map => jMap}
import clojure.java.api.Clojure
import clojure.lang.{IFn, PersistentVector, Keyword => clKeyword, Symbol => clSymbol, BigInt => clBigInt}
import datomic.Util._
import datomic.db.DbId
import us.bpsm.edn.printer.Printer.Fn
import us.bpsm.edn.printer.{Printer, Printers}

trait ClojureBridge {

  // clojure.java.api -----------------------------------------------------

  def fn(ns: String, method: String): IFn = Clojure.`var`(ns, method)

  lazy val deref = fn("clojure.core", "deref")

  lazy val requireFn     : IFn = fn("clojure.core", "require")
  lazy val referClojureFn: IFn = fn("clojure.core", "refer-clojure")

  def require(nss: String): AnyRef = requireFn.invoke(read(nss))

  def excludeSymbol(symbol: String): AnyRef = referClojureFn.invoke("exclude", read(symbol))

  def syncFn(name: String): IFn = fn("datomic.client.api", name)
  def datomicAsyncFn(name: String): IFn = fn("datomic.client.api.async", name)
  def coreAsyncFn(name: String): IFn = fn("clojure.core.async", name)

  def printLn(s: AnyRef): Unit = fn("clojure.core", "println").invoke(s)
  def readString(s: String) = fn("clojure.tools.reader", "read-string").invoke(s)


  // EDN -----------------------------------------------------------------

  // Printing edn with us.bpsm.edn
  // Adding recognition of clojure Keyword and Datomic DbId

  lazy val clPersVec: Fn[PersistentVector] = new Printer.Fn[PersistentVector]() {
    override def eval(self: PersistentVector, writer: Printer): Unit = {
      writer.append('[')
      self.forEach(o => writer.printValue(o))
      writer.append(']')
    }
  }
  lazy val clKw     : Fn[clKeyword]        = new Printer.Fn[clKeyword]() {
    override def eval(self: clKeyword, writer: Printer): Unit = {
      writer.softspace.append(self.toString).softspace
    }
  }
  lazy val clSym   : Fn[clSymbol] = new Printer.Fn[clSymbol]() {
    override def eval(self: clSymbol, writer: Printer): Unit = {
      writer.softspace.append(self.toString).softspace
    }
  }
  lazy val clBigInt: Fn[clBigInt] = new Printer.Fn[clBigInt]() {
    override def eval(self: clBigInt, writer: Printer): Unit = {
      writer.softspace.append(self.toString + "N").softspace
    }
  }
  lazy val uri     : Fn[URI]      = new Printer.Fn[URI]() {
    override def eval(self: URI, writer: Printer): Unit = {
      writer.append(s""" #=(new java.net.URI "${self.toString}")""")
    }
  }
  lazy val dbId     : Fn[DbId]             = new Printer.Fn[DbId]() {
    override def eval(self: DbId, writer: Printer): Unit = {
      if (self.idx.asInstanceOf[Long] > 0) {
        // Entity id
        // :club/member 1557968513546 :next/attr ...
        writer.printValue(self.idx)
      } else {
        // Temp id treated as text to be resolved in tx
        // :club/member"-1000001":next/attr ...
        // Skipping partition information since the client.api disregards it anyway.
        writer.printValue(self.idx.toString)
      }
    }
  }

  lazy val compact = Printers.defaultProtocolBuilder
    .put(classOf[PersistentVector], clPersVec)
    .put(classOf[clKeyword], clKw)
    .put(classOf[clSymbol], clSym)
    .put(classOf[clBigInt], clBigInt)
    .put(classOf[URI], uri)
    .put(classOf[DbId], dbId)
    .build

  lazy val pretty = Printers.prettyProtocolBuilder
    .put(classOf[PersistentVector], clPersVec)
    .put(classOf[clKeyword], clKw)
    .put(classOf[clSymbol], clSym)
    .put(classOf[clBigInt], clBigInt)
    .put(classOf[URI], uri)
    .put(classOf[DbId], dbId)
    .build


  def edn(stmts: jList[_]): String = {
    Printers.printString(compact, stmts)
  }
  def edn(stmts: jMap[_, _]): String = {
    Printers.printString(compact, stmts)
  }

  def ednPretty(stmts: jList[_]): String = {
    Printers.printString(pretty, stmts)
  }
  def ednPretty(stmts: jMap[_, _]): String = {
    Printers.printString(pretty, stmts)
  }


  // Debug methods ------------------------------

  def types(obj: Any): Unit = {
    println("-----")
    println(obj)
    println(obj.getClass)
    println(obj.getClass.getSuperclass)
    obj.getClass.getInterfaces.map(_.toString).sorted foreach println
    println("------------")
  }
}
