package datomicClojure

import datomic.Util.read

class Lookup(datomicDb: AnyRef) extends ClojureBridge {

  lazy protected val isDevLocal = datomicDb.isInstanceOf[clojure.lang.IPersistentMap]

  // todo? Implement valAt(":db-name") on dev-local?
  def dbName: String = valAt[String](if (isDevLocal) ":id" else ":db-name").get

  def basisT: Long = valAt[Long](":t").getOrElse(0)

  def asOfT: Long = valAt[Long](":as-of").getOrElse(0)

  def sinceT: Long = valAt[Long](":since").getOrElse(0)

  def isHistory: Boolean = if (isDevLocal) {
    valAt[Boolean](":history?").getOrElse(false)
  } else {
    valAt[Boolean](":history").getOrElse(false)
  }

  private def valAt[T](key: String): Option[T] = {
    // dev-local and Peer Server have different implementations
    datomicDb.asInstanceOf[clojure.lang.ILookup].valAt(read(key), "err") match {
      case null  => None // returned by dev-local
      case "err" => None // returned by Peer Server
      case v     => Some(v.asInstanceOf[T])
    }
  }
}
