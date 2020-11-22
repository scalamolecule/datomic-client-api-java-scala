package datomicScala.client.api.sync

import java.util.stream.{Stream => jStream}
import java.util.{Map => jMap}
import clojure.lang.{PersistentArrayMap, PersistentVector}
import datomic.Util
import datomic.Util._
import datomicScala.Spec
import datomicScala.client.api.{Datom, DbStats}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._


class DbTest extends Spec {

  "stats" in new Setup {
    val db: Db = conn.db
    db.dbName === "hello"
    db.basisT === tAfter
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


  "as-of lookup" in new Setup {
    val db: Db = conn.db

    db.asOf(tBefore).dbName === "hello"
    db.asOf(tBefore).basisT === tAfter
    db.asOf(tBefore).asOfT === tBefore
    db.asOf(tBefore).sinceT === 0
    db.asOf(tBefore).isHistory === false

    db.asOf(txBefore).basisT === tAfter
    db.asOf(txBefore).sinceT === 0

    db.asOf(txInstBefore).basisT === tAfter
    db.asOf(txInstBefore).sinceT === 0

    if (isDevLocal) {
      db.asOf(txBefore).asOfT === tBefore
      db.asOf(txInstBefore).asOfT === tBefore
      // Can't retrieve txInst when `asOf` initiated with Date
      // db.asOf(txInstBefore).asOfInst === txInstBefore
    } else {
      // tx returned instead of t
      db.asOf(txBefore).asOfT === txBefore

      // Can't retrieve t when `asOf` initiated with Date
      // db.asOf(txInstBefore).asOfT === tBefore
      // txInst returned instead of t
      db.asOf(txInstBefore).asOfInst === txInstBefore
    }

    db.asOf(tAfter).basisT === tAfter
    db.asOf(tAfter).asOfT === tAfter
    db.asOf(tAfter).sinceT === 0

    db.asOf(txAfter).basisT === tAfter
    db.asOf(txAfter).sinceT === 0

    db.asOf(txInstAfter).basisT === tAfter
    db.asOf(txInstAfter).sinceT === 0

    if (isDevLocal) {
      db.asOf(txAfter).asOfT === tAfter
      db.asOf(txInstAfter).asOfT === tAfter
      // Can't retrieve txInst when `asOf` initiated with Date
      // db.asOf(txInstAfter).asOfInst === txInstAfter
    } else {
      db.asOf(txAfter).asOfT === txAfter // tx !
      // db.asOf(txInstAfter).asOfT === tAfter // Date not a t
      db.asOf(txInstAfter).asOfInst === txInstAfter // txInst !
    }
  }


  "since lookup" in new Setup {
    val db: Db = conn.db

    db.since(tBefore).dbName === "hello"
    db.since(tBefore).basisT === tAfter
    db.since(tBefore).asOfT === 0
    db.since(tBefore).sinceT === tBefore
    db.since(tBefore).isHistory === false

    db.since(txBefore).basisT === tAfter
    db.since(txBefore).asOfT === 0

    db.since(txInstBefore).basisT === tAfter
    db.since(txInstBefore).asOfT === 0

    if (isDevLocal) {
      db.since(txBefore).sinceT === tBefore
      db.since(txInstBefore).sinceT === tBefore
      // Can't retrieve txInst when `since` initiated with Date
      // db.since(txInstBefore).sinceInst === txInstBefore
    } else {
      // tx returned instead of t
      db.since(txBefore).sinceT === txBefore

      // Can't retrieve t when `since` initiated with Date
      // db.since(txInstBefore).sinceT === tBefore
      // txInst returned instead of t
      db.since(txInstBefore).sinceInst === txInstBefore
    }

    db.since(tAfter).basisT === tAfter
    db.since(tAfter).asOfT === 0
    db.since(tAfter).sinceT === tAfter

    db.since(txAfter).basisT === tAfter
    db.since(txAfter).asOfT === 0

    db.since(txInstAfter).basisT === tAfter
    db.since(txInstAfter).asOfT === 0

    if (isDevLocal) {
      db.since(txAfter).sinceT === tAfter
      db.since(txInstAfter).sinceT === tAfter
      // Can't retrieve txInst when `since` initiated with Date
      // db.since(txInstAfter).sinceInst === txInstAfter

    } else {
      // tx returned instead of t
      db.since(txAfter).sinceT === txAfter

      // Can't retrieve t when `since` initiated with Date
      // db.since(txInstAfter).sinceT === tAfter
      // txInst returned instead of t
      db.since(txInstAfter).sinceInst === txInstAfter
    }
  }


  "history lookup" in new Setup {
    val db: Db = conn.db
    db.history.dbName === "hello"
    db.history.basisT === tAfter
    db.history.asOfT === 0
    db.history.sinceT === 0
    db.history.isHistory === true
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


  "with" in new Setup {
    val originalDb = conn.db

    // Test adding a 4th film
    // OBS: Note that a `conn.withDb` has to be passed initially!
    val txReport4films: TxReport = originalDb.`with`(conn.withDb, film4)
    val db4Films                 = txReport4films.dbAfter
    films(db4Films) === fourFilms

    // Add 5th film by passing with-modified Db
    val txReport5films = originalDb.`with`(db4Films, film5)
    val db5Films       = txReport5films.dbAfter
    films(db5Films) === fiveFilms

    // Add 6th film by passing with-modified Db from TxReport
    val txReport6films = originalDb.`with`(txReport5films, film6)
    val db6Films       = txReport6films.dbAfter
    films(db6Films) === sixFilms

    // Original state is unaffected
    films(originalDb) === threeFilms
  }


  "with - single invocation" in new Setup {
    // As a convenience, a single-invocation shorter version of `with`:
    films(conn.widh(film4)) === fourFilms

    // Applying another data set still augments the original db
    films(conn.widh(film5)) === (threeFilms :+ "Film 5").sorted

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


  "datoms" in new Setup {
    conn.db.datoms(
      ":avet",
      list(read(":movie/title"))
    ).toScala(List).map(_.v.toString).sorted === threeFilms
  }


  "indexRange" in new Setup {
    conn.db.indexRange(":movie/title").toScala(List).sortBy(_.e) === List(
      Datom(e1, a1, "The Goonies", txAfter, true),
      Datom(e2, a1, "Commando", txAfter, true),
      Datom(e3, a1, "Repo Man", txAfter, true),
    )
  }


  "pull" in new Setup {
    conn.db.pull("[*]", e3).toString ===
      s"""{:db/id $e3, :movie/title "Repo Man", :movie/genre "punk dystopia", :movie/release-year 1984}"""

    conn.db.pull("[*]", e3, 1000).toString ===
      s"""{:db/id $e3, :movie/title "Repo Man", :movie/genre "punk dystopia", :movie/release-year 1984}"""

    // dev-local in-memory db will pull within 1 ms
    if (!isDevLocal) {
      conn.db.pull("[*]", e3, 1) must throwA(
        new clojure.lang.ExceptionInfo(
          "Datomic Client Timeout",
          new PersistentArrayMap(
            Array(
              read(":cognitect.anomalies/category"), read(":cognitect.anomalies/interrupted"),
              read(":cognitect.anomalies/message"), "Datomic Client Timeout"
            )
          )
        )
      )
    }
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