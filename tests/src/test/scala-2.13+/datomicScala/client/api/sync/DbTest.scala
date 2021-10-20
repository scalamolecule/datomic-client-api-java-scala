package datomicScala.client.api.sync

import java.io.FileReader
import java.util.stream.{Stream => jStream}
import java.util.{Map => jMap}
import clojure.lang.PersistentVector
import datomic.Util
import datomic.Util._
import datomicClient.ErrorMsg
import datomicScala.Spec
import datomicScala.client.api.{Datom, DbStats}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._


class DbTest extends Spec {

  "stats" in new Setup {
    val db: Db = conn.db
    db.dbName === "hello"
    db.t === tAfter
    db.asOfT === 0
    db.sinceT === 0
    db.isHistory === false

    if (isDevLocal) {
      db.dbStats === DbStats(
        243,
        Some(Map(
          ":db.install/partition" -> 3,
          ":db.install/valueType" -> 15,
          ":db.install/attribute" -> 30,
          ":db/ident" -> 58,
          ":db/valueType" -> 30,
          ":db/tupleType" -> 2,
          ":db/cardinality" -> 30,
          ":db/unique" -> 1,
          ":db/doc" -> 42,
          ":db/txInstant" -> 7,
          ":db/fulltext" -> 1,
          ":fressian/tag" -> 15,
          ":movie/genre" -> 3,
          ":movie/release-year" -> 3,
          ":movie/title" -> 3,
        ))
      )
    } else {
      // Peer server db is not re-created on each test,
      // so we can only test some stable values
      db.dbStats.datoms >= 0
      db.dbStats.attrs.get(":db.install/partition") === 3
    }
  }


  "Db lookup" >> {

    "Lookup as-of" in new Setup {
      // Make sure there's at leas 1 ms between dates
      Thread.sleep(5)
      val txReport4 = conn.transact(film4)
      Thread.sleep(5)
      val txReport5 = conn.transact(film5)
      val db        = conn.db

      val t4basis = txReport4.basisT
      val t4      = txReport4.t
      val tx4     = txReport4.tx
      val txInst4 = txReport4.txInst

      val t5basis = txReport5.basisT
      val t5      = txReport5.t
      val tx5     = txReport5.tx
      val txInst5 = txReport5.txInst

      // db name and history unaffected by as-of filter
      db.dbName === "hello"
      db.asOf(t4).dbName === "hello"

      db.isHistory === false
      db.asOf(t4).isHistory === false

      // basis-t is the t of the most recent transaction
      t4basis === tAfter
      t5basis === t4

      // Current t is that of transaction 5
      db.t === t5

      // t of as-of db is still the same as the un-filtered db
      db.asOf(t4).t === db.t
      db.asOf(tx4).t === db.t
      db.asOf(txInst4).t === db.t

      db.asOf(t5).t === db.t
      db.asOf(tx5).t === db.t
      db.asOf(txInst5).t === db.t

      // Use asOfT to retrieve as-of t of filtered db
      db.asOf(t4).asOfT === t4
      db.asOf(tx4).asOfT === t4
      db.asOf(txInst4).asOfT === t4

      db.asOf(t5).asOfT === t5
      db.asOf(tx5).asOfT === t5
      db.asOf(txInst5).asOfT === t5

      // Un-filtered db has no as-of t
      db.asOfT === 0

      // Use asOfTxInst to retrieve as-of tx instant of filtered db
      db.asOf(t4).asOfTxInst === txInst4
      db.asOf(tx4).asOfTxInst === txInst4
      db.asOf(txInst4).asOfTxInst === txInst4

      db.asOf(t5).asOfTxInst === txInst5
      db.asOf(tx5).asOfTxInst === txInst5
      db.asOf(txInst5).asOfTxInst === txInst5


      // as-of-filtered db has no sinceT
      db.asOf(t4).sinceT === 0
      db.asOf(tx4).sinceT === 0
      db.asOf(txInst4).sinceT === 0
      db.asOf(t5).sinceT === 0
      db.asOf(tx5).sinceT === 0
      db.asOf(txInst5).sinceT === 0

      // as-of-filtered db has no sinceTxInst
      db.asOf(t4).sinceTxInst === null
      db.asOf(tx4).sinceTxInst === null
      db.asOf(txInst4).sinceTxInst === null
      db.asOf(t5).sinceTxInst === null
      db.asOf(tx5).sinceTxInst === null
      db.asOf(txInst5).sinceTxInst === null
    }


    "Lookup since" in new Setup {
      // Make sure there's at leas 1 ms between dates
      Thread.sleep(5)
      val txReport4 = conn.transact(film4)
      Thread.sleep(5)
      val txReport5 = conn.transact(film5)
      val db        = conn.db

      val t4basis = txReport4.basisT
      val t4      = txReport4.t
      val tx4     = txReport4.tx
      val txInst4 = txReport4.txInst

      val t5basis = txReport5.basisT
      val t5      = txReport5.t
      val tx5     = txReport5.tx
      val txInst5 = txReport5.txInst

      // db name and history unaffected by as-of filter
      db.dbName === "hello"
      db.since(t4).dbName === "hello"

      db.isHistory === false
      db.since(t4).isHistory === false

      // basis-t is the t of the most recent transaction
      t4basis === tAfter
      t5basis === t4

      // Current t is that of transaction 5
      db.t === t5

      // t of since db is still the same as the un-filtered db
      db.since(t4).t === db.t
      db.since(tx4).t === db.t
      db.since(txInst4).t === db.t

      db.since(t5).t === db.t
      db.since(tx5).t === db.t
      db.since(txInst5).t === db.t

      // Use sinceT to retrieve since t of filtered db
      db.since(t4).sinceT === t4
      db.since(tx4).sinceT === t4
      db.since(txInst4).sinceT === t4

      db.since(t5).sinceT === t5
      db.since(tx5).sinceT === t5
      db.since(txInst5).sinceT === t5

      // Un-filtered db has no since t
      db.sinceT === 0

      // Use sinceTxInst to retrieve since tx instant of filtered db.
      // Normally this is not available since the since-filtered db excludes
      // the time point, but we are cache it when applying the filter on the
      // still unfiltered db.
      db.since(t4).sinceTxInst === txInst4
      db.since(tx4).sinceTxInst === txInst4
      db.since(txInst4).sinceTxInst === txInst4

      db.since(t5).sinceTxInst === txInst5
      db.since(tx5).sinceTxInst === txInst5
      db.since(txInst5).sinceTxInst === txInst5


      // since-filtered db has no asOfT
      db.since(t4).asOfT === 0
      db.since(tx4).asOfT === 0
      db.since(txInst4).asOfT === 0
      db.since(t5).asOfT === 0
      db.since(tx5).asOfT === 0
      db.since(txInst5).asOfT === 0

      // since-filtered db has no asOfTxInst
      db.since(t4).asOfTxInst === null
      db.since(tx4).asOfTxInst === null
      db.since(txInst4).asOfTxInst === null
      db.since(t5).asOfTxInst === null
      db.since(tx5).asOfTxInst === null
      db.since(txInst5).asOfTxInst === null
    }


    "Lookup history" in new Setup {
      val db = conn.db
      db.history.dbName === "hello"
      db.history.t === tAfter
      db.history.asOfT === 0
      db.history.sinceT === 0
      db.history.isHistory === true
    }
  }


  "as-of" in new Setup {
    // Current state
    films(conn.db) === threeFilms

    // State before last tx
    films(conn.db.asOf(tBefore)) === Nil
    films(conn.db.asOf(txBefore)) === Nil
    films(conn.db.asOf(txInstBefore)) === Nil

    // State after last tx same as current state
    films(conn.db.asOf(tAfter)) === threeFilms
    films(conn.db.asOf(txAfter)) === threeFilms
    films(conn.db.asOf(txInstAfter)) === threeFilms

    // We can use the transaction id too
    films(conn.db.asOf(txBefore)) === Nil
  }


  "since" in new Setup {
    // State created since previous t
    films(conn.db.since(tBefore)) === threeFilms
    films(conn.db.since(txBefore)) === threeFilms
    films(conn.db.since(txInstBefore)) === threeFilms

    // Nothing created after
    films(conn.db.since(tAfter)) === Nil
    films(conn.db.since(txAfter)) === Nil
    films(conn.db.since(txInstAfter)) === Nil
  }


  "with java stmts" in new Setup {
    // Original state
    films(conn.db) === threeFilms

    // Test adding a 4th film
    // OBS: Note that a `conn.withDb` has to be passed initially!
    val txReport4films = conn.db.`with`(conn.withDb, film4)
    val db4Films       = txReport4films.dbAfter
    films(db4Films) === fourFilms

    // Add 5th film by passing with-modified Db
    val txReport5films = conn.db.`with`(db4Films, film5)
    val db5Films       = txReport5films.dbAfter
    films(db5Films) === fiveFilms

    // Add 6th film by passing with-modified Db from TxReport
    val txReport6films = conn.db.`with`(txReport5films, film6)
    val db6Films       = txReport6films.dbAfter
    films(db6Films) === sixFilms

    // Combining `with` and `asOf`
    // todo: peer-server doesn't allow combining `with` filter with other filters
    if (system == "dev-local")
      films(db6Films.asOf(txReport5films.tx)) === fiveFilms

    // Original state is unaffected
    films(conn.db) === threeFilms
  }

  "with edn file" in new Setup {
    // Original state
    films(conn.db) === threeFilms

    val txReport4films = conn.db.`with`(conn.withDb,
      new FileReader("tests/resources/film4.edn")
    )
    val db4Films       = txReport4films.dbAfter
    films(db4Films) === fourFilms

    // Add 5th film by passing with-modified Db
    val txReport5films = conn.db.`with`(db4Films,
      new FileReader("tests/resources/film5.edn")
    )
    val db5Films       = txReport5films.dbAfter
    films(db5Films) === fiveFilms

    // Add 6th film by passing with-modified Db from TxReport
    val txReport6films = conn.db.`with`(txReport5films,
      new FileReader("tests/resources/film6.edn")
    )
    val db6Films       = txReport6films.dbAfter
    films(db6Films) === sixFilms

    // Combining `with` and `asOf`
    // todo: peer-server doesn't allow combining `with` filter with other filters
    if (system == "dev-local")
      films(db6Films.asOf(txReport5films.tx)) === fiveFilms

    // Original state is unaffected
    films(conn.db) === threeFilms
  }

  "with edn string" in new Setup {
    // Original state
    films(conn.db) === threeFilms

    val txReport4films = conn.db.`with`(conn.withDb,
      """[ {:movie/title "Film 4"} ]"""
    )
    val db4Films       = txReport4films.dbAfter
    films(db4Films) === fourFilms

    // Add 5th film by passing with-modified Db
    val txReport5films = conn.db.`with`(db4Films,
      """[ {:movie/title "Film 5"} ]"""
    )
    val db5Films       = txReport5films.dbAfter
    films(db5Films) === fiveFilms

    // Add 6th film by passing with-modified Db from TxReport
    val txReport6films = conn.db.`with`(txReport5films,
      """[ {:movie/title "Film 6"} ]"""
    )
    val db6Films       = txReport6films.dbAfter
    films(db6Films) === sixFilms

    // Combining `with` and `asOf`
    // todo: peer-server doesn't allow combining `with` filter with other filters
    if (system == "dev-local")
      films(db6Films.asOf(txReport5films.tx)) === fiveFilms

    // Original state is unaffected
    films(conn.db) === threeFilms
  }


  "with java stmts - single invocation" in new Setup {
    // As a convenience, a single-invocation shorter version of `with`:
    films(conn.widh(film4)) === fourFilms

    // Applying another data set still augments the original db
    films(conn.widh(film4and5)) === fiveFilms

    // Current state is unaffected
    films(conn.db) === threeFilms
  }

  "with edn file - single invocation" in new Setup {
    // As a convenience, a single-invocation shorter version of `with`:
    films(conn.widh(
      new FileReader("tests/resources/film4.edn")
    )) === fourFilms

    // Applying another data set still augments the original db
    films(conn.widh(
      new FileReader("tests/resources/film4and5.edn")
    )) === fiveFilms

    // Current state is unaffected
    films(conn.db) === threeFilms
  }

  "with edn string - single invocation" in new Setup {
    // As a convenience, a single-invocation shorter version of `with`:
    films(conn.widh(
      """[ {:movie/title "Film 4"} ]"""
    )) === fourFilms

    // Applying another data set still augments the original db
    films(conn.widh(
      """[ {:movie/title "Film 4"} {:movie/title "Film 5"} ]"""
    )) === fiveFilms

    // Current state is unaffected
    films(conn.db) === threeFilms
  }


  "history" in new Setup {

    // Not testing Peer Server history since history is accumulating when we
    // can't re-create database for each test without shutting down Peer Server.

    if (isDevLocal) {

      // Current and history db are currently the same
      films(conn.db) === threeFilms
      films(conn.db.history) === threeFilms

      // As long as we only add data, current/history will be the same
      val tx = conn.transact(film4)
      films(conn.db) === fourFilms
      films(conn.db.history) === fourFilms

      // Now retract the last entity
      val retractedEid = tx.txData.toScala(List).last.e
      conn.transact(list(list(read(":db/retractEntity"), retractedEid)))
      films(conn.db) === threeFilms
      films(conn.db.history) === fourFilms

      // History of movie title assertions and retractions
      Datomic.q(
        """[:find ?movie-title ?tx ?added
          |:where [_ :movie/title ?movie-title ?tx ?added]]""".stripMargin,

        // Use history database
        conn.db.history
      ).asInstanceOf[PersistentVector].asScala.toList
        .map { row =>
          val List(v, tx, added) = row.asInstanceOf[PersistentVector].asScala.toList
          (v.toString, tx.asInstanceOf[Long], added.asInstanceOf[Boolean])
        }.sortBy(_._2) ===
        List(
          // First tx
          ("Repo Man", 13194139533319L, true),
          ("The Goonies", 13194139533319L, true),
          ("Commando", 13194139533319L, true),

          // Film 4 added
          ("Film 4", 13194139533320L, true),

          // Film 4 retracted
          ("Film 4", 13194139533321L, false)
        )
    }
  }


  "datoms" >> {

    // Datoms from specified index
    // Optionally filter by components of the index.

    "datoms AVET" in new Setup {

      // AVET index is sorted by
      // Attribute (id, not name!) - Value - Entity id - Transaction id

      // Supply A value (as clojure.lang.Keyword)
      // Get all datoms of attribute :movie/title
      conn.db.datoms(
        ":avet",
        list(read(":movie/title"))
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true),
        Datom(e3, a1, "Repo Man", txAfter, true),
        Datom(e1, a1, "The Goonies", txAfter, true)
      )

      // A and V
      conn.db.datoms(
        ":avet",
        list(read(":movie/title"), "Commando")
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )

      // A, V and E (e2 is the eid of the Commando film entity)
      conn.db.datoms(
        ":avet",
        list(read(":movie/title"), "Commando", e2)
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )

      // A, V, E and T (tAfter is the time point of the film saving tx)
      // Time point T can be a t or tx (not a txInstant / Date)
      conn.db.datoms(
        ":avet",
        list(read(":movie/title"), "Commando", e2, tAfter)
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )

      // (only dev-local is re-created on each test and therefore has a stable size)
      if (system == "dev-local") {
        // We can supply an empty components list and get the entire (!) db (requires
        // though to set limit = -1)
        conn.db.datoms(
          ":avet",
          list(),
          limit = -1 // to fetch all!
        ).toScala(List).size === 243
      }

      // limit number of datoms returned
      conn.db.datoms(
        ":avet",
        list(read(":movie/title")),
        limit = 2
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true),
        Datom(e3, a1, "Repo Man", txAfter, true)
      )

      // Add offset for first datom in index to return
      conn.db.datoms(
        ":avet",
        list(read(":movie/title")),
        offset = 1,
        limit = 2
      ).toScala(List) === List(
        Datom(e3, a1, "Repo Man", txAfter, true),
        Datom(e1, a1, "The Goonies", txAfter, true)
      )
    }


    "datoms EAVT" in new Setup {

      // EAVT index is sorted by
      // Entity id - Attribute id (not name!) - Value - Transaction id

      // E - Get all datoms of entity e2
      conn.db.datoms(
        ":eavt",
        list(e2)
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true),
        Datom(e2, a2, "thriller/action", txAfter, true),
        Datom(e2, a3, 1985, txAfter, true)
      )

      // EA
      conn.db.datoms(
        ":eavt",
        list(e2, read(":movie/title"))
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )

      // EAV
      conn.db.datoms(
        ":eavt",
        list(e2, read(":movie/title"), "Commando")
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )

      // EAVT
      conn.db.datoms(
        ":eavt",
        list(e2, read(":movie/title"), "Commando", txAfter)
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )
    }


    "datoms AEVT" in new Setup {

      // AEVT index is sorted by
      // Attribute id (not name!) - Entity id - Value - Transaction id

      // A
      conn.db.datoms(
        ":aevt",
        list(read(":movie/title"))
      ).toScala(List) === List(
        Datom(e1, a1, "The Goonies", txAfter, true),
        Datom(e2, a1, "Commando", txAfter, true),
        Datom(e3, a1, "Repo Man", txAfter, true),
      )

      // AE
      conn.db.datoms(
        ":aevt",
        list(read(":movie/title"), e2)
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )

      // AEV
      conn.db.datoms(
        ":aevt",
        list(read(":movie/title"), e2, "Commando")
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )

      // AEVT
      //      val l = list(read(":movie/title"), e2, "Commando", txAfter)

      conn.db.datoms(
        ":aevt",
        list(read(":movie/title"), e2, "Commando", txAfter)
      ).toScala(List) === List(
        Datom(e2, a1, "Commando", txAfter, true)
      )
    }


    "datoms VAET" in new Setup {

      // The VAET index is for following relationships in reverse.
      // No ref type is defined here, but it follows the same pattern as
      // shown above.
      // See https://docs.datomic.com/cloud/query/raw-index-access.html#vaet

      // VAET index is sorted by
      // Value (ref value) - Attribute id (not name!) - Entity id - Transaction id

      ok
    }
  }


  def indexRange(
    attrId: String,
    startValue: Option[Any] = None,
    endValue: Option[Any] = None,
    timeout: Int = 0,
    offset: Int = 0,
    limit: Int = 1000
  ): List[String] = conn.db.indexRange(
    attrId, startValue, endValue, timeout, offset, limit
  ).toScala(List).map(_.v.toString)


  "indexRange" in new Setup {

    // Datoms from AVET index sorted by attribute-value-entity-tx

    // indexRange allows to narrow the datoms selection withing a value range
    // from start value, inclusive, until end value, exclusive.

    // This retrieves the first 1000 datoms for :movie/title
    conn.db.indexRange(":movie/title").toScala(List) === List(
      Datom(e2, a1, "Commando", txAfter, true),
      Datom(e3, a1, "Repo Man", txAfter, true),
      Datom(e1, a1, "The Goonies", txAfter, true),
    )

    // Retrieve all (!) datoms for :movie/title (in this case just 3)
    conn.db.indexRange(":movie/title", limit = -1).toScala(List) === List(
      Datom(e2, a1, "Commando", txAfter, true),
      Datom(e3, a1, "Repo Man", txAfter, true),
      Datom(e1, a1, "The Goonies", txAfter, true),
    )

    // For brevity, only the v value of the datoms is shown below...

    // Range --------------------

    // Titles sorting after C (no end value)
    indexRange(":movie/title", Some("C")) === List("Commando", "Repo Man", "The Goonies")

    // Titles sorting after Cu
    indexRange(":movie/title", Some("Cu")) === List("Repo Man", "The Goonies")

    // Titles after D (no start value)
    indexRange(":movie/title", Some("D")) === List("Repo Man", "The Goonies")

    // Titles after d - case-sensitivity regards small letters after capital letters
    indexRange(":movie/title", Some("c")) === List()

    // Titles before S
    indexRange(":movie/title", None, Some("S")) === List("Commando", "Repo Man")

    // Titles before T ("The Goonies" is before "T")
    indexRange(":movie/title", None, Some("T")) === List("Commando", "Repo Man")

    // Titles after C, before S
    indexRange(":movie/title", Some("C"), Some("S")) === List("Commando", "Repo Man")

    // Titles after D, before S
    indexRange(":movie/title", Some("D"), Some("S")) === List("Repo Man")


    // Limit --------------------

    indexRange(":movie/title", limit = 2) === List("Commando", "Repo Man")
    indexRange(":movie/title", limit = 1) === List("Commando")
    indexRange(":movie/title", limit = 0) must throwA(
      new IllegalArgumentException(ErrorMsg.limit)
    )
    // Take all (!)
    indexRange(":movie/title", limit = -1) === List("Commando", "Repo Man", "The Goonies")


    // Offset --------------------

    indexRange(":movie/title", offset = 0) === List("Commando", "Repo Man", "The Goonies")

    // Commando is skipped
    indexRange(":movie/title", offset = 1) === List("Repo Man", "The Goonies")

    // Notice that offset is from current range (Repo Man is skipped)
    indexRange(":movie/title", Some("D"), offset = 1) === List("The Goonies")

    indexRange(":movie/title", offset = 1, limit = 1) === List("Repo Man")
  }


  "pull" in new Setup {
    conn.db.pull("[*]", e3).toString ===
      s"""{:db/id $e3, :movie/title "Repo Man", :movie/genre "punk dystopia", :movie/release-year 1984}"""

    conn.db.pull("[*]", e3, 1000).toString ===
      s"""{:db/id $e3, :movie/title "Repo Man", :movie/genre "punk dystopia", :movie/release-year 1984}"""

    // Both dev-local and peer-server inmem pull within 1 ms, so we can't test it here
    //    conn.db.pull("[*]", e3, 1) must throwA(
    //      Interrupted("Datomic Client Timeout")
    //    )
  }


  // since 1.0.61.65
  "indexPull" in new Setup {

    // Pull from :avet index
    val javaStream: jStream[_] = conn.db.indexPull(
      ":avet",
      "[:movie/title :movie/release-year]",
      "[:movie/release-year 1985]"
    )

    // Lazily convert to scala LazyList
    val scalaLazyList: LazyList[Any] = javaStream.toScala(LazyList)

    // Evaluate lazy list
    scalaLazyList.size === 2

    // LazyList in Scala can be accessed multiple times contrary to java Stream
    // that can only be consumed once (like an Iterator).
    val firstFilm = scalaLazyList.head

    // The underlying type of pulled indexes are clojure.lang.PersistentArrayMaps
    // that need to be cast to java Maps before we can access values of the map.
    firstFilm.getClass === classOf[clojure.lang.PersistentArrayMap]
    val firstFilmMap = firstFilm.asInstanceOf[jMap[_, _]]

    firstFilmMap.get(read(":movie/title")) === "The Goonies"


    // Comparing with data structures...

    // We might think that we can compare with a data structure:
    val firstFilmData: jMap[_, _] = Util.map(
      read(":movie/title"), "The Goonies",
      read(":movie/release-year"), 1985
    )
    // But we can't
    firstFilmMap !== firstFilmData

    // That's because the underlying index types are different:
    firstFilmMap.getClass === classOf[clojure.lang.PersistentArrayMap]
    firstFilmData.getClass.toString === "class java.util.Collections$UnmodifiableMap"

    // Their String representations differ:
    firstFilmMap.toString ===
      """{:movie/title "The Goonies", :movie/release-year 1985}"""
    firstFilmData.toString ===
      """{:movie/title=The Goonies, :movie/release-year=1985}"""
  }
}