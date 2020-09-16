package datomicJava.client.api.sync

import java.util
import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.Keyword
import datomic.Util._
import datomicClojure.Invoke
import datomicJava.client.api.{Datom, DbStats}
import datomicJava.util.Helper._
import datomicJava.util.Helper.streamOfDatoms
import scala.jdk.CollectionConverters._


case class Db(datomicDb: AnyRef) {

  lazy private val isDevLocal = datomicDb.isInstanceOf[datomic.core.db.Db]

  // Db info methods --------------------------------------

  // todo? Implement valAt(":db-name") on dev-local?
  def dbName: String = valAt[String](if (isDevLocal) ":id" else ":db-name").get

  def basisT: Long = valAt[Long](":t").getOrElse(0)

  def asOfT: Long = valAt[Long](":as-of").getOrElse(0)

  def sinceT: Long = valAt[Long](":since").getOrElse(0)

  // todo? Implement valAt(":history") on dev-local?
  def isHistory: Boolean = if (isDevLocal)
    datomicDb.asInstanceOf[datomic.core.db.Db].isHistory
  else
    valAt[Boolean](":history").getOrElse(false)

  private def valAt[T](key: String): Option[T] = {
    // dev-local and Peer Server have different implementations
    datomicDb.asInstanceOf[clojure.lang.ILookup].valAt(read(key), "err") match {
      case null  => None // returned by dev-local
      case "err" => None // returned by Peer Server
      case v     => Some(v.asInstanceOf[T])
    }
  }

  def dbStats: DbStats = {
    val raw      = Invoke.dbStats(datomicDb)
    val datoms   = raw.get(read(":datoms")).asInstanceOf[Long]
    val attrsRaw = raw.get(read(":attrs"))
    if (attrsRaw != null) {
      val jmap  = new util.HashMap[String, Long]()
      val count = if (isDevLocal)
        (rawCount: Any) => rawCount.asInstanceOf[Int].toLong
      else
        (rawCount: Any) => rawCount.asInstanceOf[Long]

      // todo? Count value type differs: DevLocal: Integer, PeerServer: Long
      attrsRaw.asInstanceOf[jMap[_, _]].asScala.toMap.foreach {
        case (kw: Keyword, m: jMap[_, _]) =>
          jmap.put(kw.toString, count(m.asScala.head._2))

        case otherPair => throw new RuntimeException(
          "Unexpected pair from db-stats: " + otherPair
        )
      }

      new DbStats(datoms, jmap)
    } else {
      new DbStats(datoms, null)
    }
  }


  // Time filters --------------------------------------

  def asOf(t: Long): Db = Db(Invoke.asOf(datomicDb, t))

  def since(t: Long): Db = Db(Invoke.since(datomicDb, t))

  def `with`(withDb: AnyRef, stmts: jList[_]): Db = {
    if (withDb.isInstanceOf[Db])
      throw new IllegalArgumentException(
        """Please pass a "with-db", initially created from `conn.withDb` and """ +
          "subsequently with `<Db-object>.datomicDb`.")
    Db(
      Invoke.`with`(withDb, stmts).get(read(":db-after")).asInstanceOf[AnyRef]
    )
  }

  def history: Db = Db(Invoke.history(datomicDb))


  // Indexes --------------------------------------

  /**
   *
   * @param index      String :eavt, :aevt, :avet, or :vaet
   * @param components Optional vector in the same order as the index
   *                   containing one or more values to further narrow the
   *                   result.
   * @return List[datomicFacade.client.api.Datom] Wrapped Datoms with a unified api
   */
  def datoms(index: String, components: jList[_]): jStream[Datom] = {
    streamOfDatoms(
      Invoke.datoms(datomicDb, index, components)
    )
  }

  def indexRange[T](attrId: String): jStream[Datom] =
    indexRange(attrId, None, None, 0, 0, 1000)

  def indexRange[T](attrId: String, start: Option[Any]): jStream[Datom] =
    indexRange(attrId, start, None, 0, 0, 1000)

  def indexRange[T](
    attrId: String,
    start: Any,
    end: Any
  ): jStream[Datom] = indexRange(attrId, start, end, 0, 0, 1000)


  def indexRange(
    attrId: String,
    start: Any,
    end: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jStream[Datom] = {
    streamOfDatoms(
      Invoke.indexRange(datomicDb, attrId, Option(start), Option(end), timeout, offset, limit)
    )
  }


  // Pull --------------------------------------

  def pull(selector: String, eid: Any): jMap[_, _] =
    pull(selector, eid, 0, 0, 1000)

  def pull(selector: String, eid: Any, limit: Int): jMap[_, _] =
    pull(selector, eid, 0, 0, limit)

  def pull(
    selector: String,
    eid: Any,
    timeout: Int,
    offset: Int,
    limit: Int
  ): jMap[_, _] = {
    Invoke.pull(datomicDb, selector, eid, timeout, offset, limit)
  }


  def indexPull(
    index: String,
    selector: String,
    start: String
  ): jStream[_] = {
    indexPull(index, selector, start, false, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean
  ): jStream[_] = {
    indexPull(index, selector, start, reverse, 0, 0, 1000)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    limit: Int,
  ): jStream[_] = {
    indexPull(index, selector, start, reverse, 0, 0, limit)
  }

  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Boolean,
    timeout: Int,
    offset: Int,
    limit: Int,
  ): jStream[_] = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException("Index can only be :avet or :aevt")
    Invoke.indexPull(
      datomicDb, index, selector, start, reverse, timeout, offset, limit
    )
  }
}

object Db {
  def apply(conn: Connection): Db = Db(conn.db.datomicDb)
  def apply(db: Db): Db = Db(db.datomicDb)
}