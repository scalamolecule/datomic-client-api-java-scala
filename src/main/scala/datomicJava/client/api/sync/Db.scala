package datomicJava.client.api.sync

import java.util
import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.Keyword
import datomic.Util._
import datomicJava.client.api.{Datom, DbStats}
import datomicJava.util.Helper._
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
    val raw      = syncFn("db-stats").invoke(datomicDb).asInstanceOf[jMap[_, _]]
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

  def asOf(t: Long): Db = Db(syncFn("as-of").invoke(datomicDb, t))

  def since(t: Long): Db = Db(syncFn("since").invoke(datomicDb, t))

  def `with`(withDb: AnyRef, list: jList[_]): Db = {
    if (withDb.isInstanceOf[Db])
      throw new IllegalArgumentException(
        """Please pass a "with-db", initially created from `conn.withDb` and """ +
          "subsequently with `<Db-object>.datomicDb`.")
    Db(
      syncFn("with").invoke(
        withDb,
        read(s"{:tx-data ${edn(list)}}")
      ).asInstanceOf[jMap[_, _]]
        .get(read(":db-after")).asInstanceOf[AnyRef]
    )
  }

  def history: Db = Db(syncFn("history").invoke(datomicDb))


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
    // dev-local: LazySeq
    // peer-server: Iterable
    streamOfDatoms(syncFn("datoms").invoke(
      datomicDb,
      read(
        s"""{
           |:index $index
           |:components ${edn(components)}
           |}""".stripMargin
      )
    ))
  }

  def indexRange[T](
    attrId: String,
    start: Option[T] = None,
    end: Option[T] = None
  ): jStream[Datom] = {
    val start_ = start.fold("")(s => s":start $s")
    val end_   = end.fold("")(e => s":end $e")
    // dev-local: LazySeq
    // peer-server: Iterable
    streamOfDatoms(
      syncFn("index-range").invoke(
        datomicDb,
        read(
          s"""{
             |:attrid $attrId
             |$start_
             |$end_
             |}""".stripMargin
        )
      )
    )
  }


  // Pull --------------------------------------

  def pull(
    selector: String,
    eid: Any,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): jMap[_, _] = {
    val timeout_ = if (timeout == 0) "" else s":timeout $timeout"
    val offset_  = if (offset == 0) "" else s":offset $offset"
    val limit_   = if (limit == 1000) "" else s":limit $limit"
    syncFn("pull").invoke(
      datomicDb,
      read(
        s"""{
           |:selector $selector
           |:eid $eid
           |$timeout_
           |$offset_
           |$limit_
           |}""".stripMargin
      )
    ).asInstanceOf[jMap[_, _]]
  }


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Option[Boolean] = None,
    timeout: Option[Int] = None,
    offset: Int = 0,
    limit: Int = 1000
  ): jStream[_] = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException("Index can only be :avet or :aevt")

    val reverse_ = reverse.fold("")(r => s":reverse $r")
    val timeout_ = timeout.fold("")(t => s":timeout $t")
    val offset_  = if (offset == 0) "" else s":offset $offset"
    val limit_   = if (limit == 1000) "" else s":limit $limit"

    syncFn("index-pull").invoke(
      datomicDb,
      read(
        s"""{
           |:index $index
           |:selector $selector
           |:start $start
           |$reverse_
           |$timeout_
           |$offset_
           |$limit_
           |}""".stripMargin
      )
    ).asInstanceOf[clojure.lang.ChunkedCons].stream()
  }
}

object Db {
  def apply(conn: Connection): Db = Db(conn.db.datomicDb)
  def apply(db: Db): Db = Db(db.datomicDb)
}