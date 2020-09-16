package datomicClojure

import java.util.{List => jList, Map => jMap}
import clojure.java.api.Clojure
import clojure.lang.{IFn, Keyword => clKeyword, Symbol => clSymbol}
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

  // Blocking take values from clojure.core.async.Channel
  //  def <!!(channel: AnyRef): AnyRef = coreAsyncFn("<!!").invoke(channel)

  //  def callAndTake(name: String)

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


  // Debug methods ------------------------------

  def types(obj: Any): Unit = {
    println("-----")
    println(obj)
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
