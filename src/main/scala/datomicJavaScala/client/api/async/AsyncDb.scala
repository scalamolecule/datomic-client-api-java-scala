package datomicJavaScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{List => jList, Map => jMap}
import clojure.lang.{Keyword, LazySeq}
import datomic.Util
import datomic.Util._
import datomicJavaScala.client.api.{Datom, DbStats}
import datomicJavaScala.util.ClojureBridge
import scala.jdk.CollectionConverters._


case class AsyncDb(datomicDb: AnyRef) extends ClojureBridge {

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

  def dbStats: Channel[DbStats] = {
    val raw    = new Channel[jMap[_, _]](
      datomicAsyncFn("db-stats").invoke(datomicDb)
    ).realize
    val datoms = raw.get(read(":datoms")).asInstanceOf[Long]

    // todo: seems to work only for dev-local
    val attrsRaw = raw.get(read(":attrs"))
    val attrs    = if (attrsRaw != null)
      Some(
        attrsRaw.asInstanceOf[jMap[_, _]].asScala.toMap.map {
          case (kw: Keyword, m: jMap[_, _]) =>
            kw.toString -> m.asScala.head._2.asInstanceOf[Int]

          case otherPair => throw new RuntimeException(
            "Unexpected pair from db-stats: " + otherPair
          )
        }
      ) else None
    new Channel[DbStats](DbStats(datoms, attrs))
  }


  // Time filters --------------------------------------

  def asOf(t: Long): AsyncDb =
    AsyncDb(datomicAsyncFn("as-of").invoke(datomicDb, t))

  def since(t: Long): AsyncDb =
    AsyncDb(datomicAsyncFn("since").invoke(datomicDb, t))

  def `with`(withDb: AnyRef, list: jList[_]): Channel[AsyncDb] = {
    if (withDb.isInstanceOf[AsyncDb])
      throw new IllegalArgumentException(
        """Please pass a "with-db", initially created from `conn.withDb` and """ +
          "subsequently with `<Db-object>.datomicDb`.")

    new Channel[AsyncDb](
      AsyncDb(
        new Channel[AnyRef](
          datomicAsyncFn("with").invoke(
            withDb,
            read(s"{:tx-data ${edn(list)}}")
          )
        ).realize.asInstanceOf[jMap[_, _]]
          .get(read(":db-after")).asInstanceOf[AnyRef]
      )
    )
  }

  def history: AsyncDb = AsyncDb(datomicAsyncFn("history").invoke(datomicDb))


  // Indexes --------------------------------------

  /**
   *
   * @param index      String :eavt, :aevt, :avet, or :vaet
   * @param components Optional vector in the same order as the index
   *                   containing one or more values to further narrow the
   *                   result.
   * @return List[datomicFacade.client.api.Datom] Wrapped Datoms with a unified api
   */
  def datoms(index: String, components: jList[_]): Channel[jStream[Datom]] = {
    // dev-local: LazySeq
    // peer-server: Iterable
    new Channel[jStream[Datom]](
      streamOfDatoms(
        new Channel[Any](
          datomicAsyncFn("datoms").invoke(
            datomicDb,
            read(
              s"""{
                 |:index $index
                 |:components ${edn(components)}
                 |}""".stripMargin
            )
          )
        ).realize
      )
    )
  }

  def indexRange[T](
    attrId: String,
    start: Option[T] = None,
    end: Option[T] = None
  ): Channel[jStream[Datom]] = {
    val start_ = start.fold("")(s => s":start $s")
    val end_   = end.fold("")(e => s":end $e")
    // dev-local: LazySeq
    // peer-server: Iterable
    new Channel[jStream[Datom]](
      streamOfDatoms(
        new Channel[Any](
          datomicAsyncFn("index-range").invoke(
            datomicDb,
            read(
              s"""{
                 |:attrid $attrId
                 |$start_
                 |$end_
                 |}""".stripMargin
            )
          )
        ).realize
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
  ): Channel[jMap[_, _]] = {
    val timeout_ = if (timeout == 0) "" else s":timeout $offset"
    val offset_  = if (offset == 0) "" else s":offset $offset"
    val limit_   = if (limit == 1000) "" else s":limit $limit"
    new Channel[jMap[_, _]](
      datomicAsyncFn("pull").invoke(
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
      )
    )
  }


  def indexPull(
    index: String,
    selector: String,
    start: String,
    reverse: Option[Boolean] = None,
    timeout: Option[Int] = None,
    offset: Int = 0,
    limit: Int = 1000
  ): Channel[jStream[_]] = {
    if (!Seq(":avet", ":aevt").contains(index))
      throw new IllegalArgumentException("Index can only be :avet or :aevt")

    val reverse_ = reverse.fold("")(r => s":reverse $r")
    val timeout_ = timeout.fold("")(t => s":timeout $t")
    val offset_  = if (offset == 0) "" else s":offset $offset"
    val limit_   = if (limit == 1000) "" else s":limit $limit"

    new Channel[jStream[_]](
      new Channel[Any](
        datomicAsyncFn("index-pull").invoke(
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
        )
      ).realize.asInstanceOf[LazySeq].stream()
    )
  }
}

object AsyncDb {
  def apply(conn: AsyncConnection): AsyncDb = AsyncDb(conn.db.datomicDb)
  def apply(db: AsyncDb): AsyncDb = AsyncDb(db.datomicDb)
}