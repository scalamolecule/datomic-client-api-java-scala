package datomicScala.client.api.async

import java.util.stream.{Stream => jStream}
import java.util.{Map => jMap}
import clojure.lang.{PersistentArrayMap, PersistentVector}
import datomic.Util
import datomic.Util._
import datomicScala.SpecAsync
import datomicScala.client.api.{Datom, DbStats}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._


class AsyncDbTest extends SpecAsync {
  sequential


  "stats" in new AsyncSetup {
    val db = conn.db

    db.dbName === "hello"
    db.basisT === tAfter
    db.asOfT === 0
    db.sinceT === 0
    db.isHistory === false

    if (isDevLocal) {
      waitFor(db.dbStats).toOption.get === DbStats(
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
      val dbStats = waitFor(db.dbStats).toOption.get
      dbStats.datoms >= 0
      dbStats.attrs.get(":db.install/partition") === 3
    }

    db.asOf(tBefore).dbName === "hello"
    db.asOf(tBefore).basisT === tAfter
    db.asOf(tBefore).asOfT === tBefore
    db.asOf(tBefore).sinceT === 0
    db.asOf(tBefore).isHistory === false

    db.asOf(tAfter).dbName === "hello"
    db.asOf(tAfter).basisT === tAfter
    db.asOf(tAfter).asOfT === tAfter
    db.asOf(tAfter).sinceT === 0
    db.asOf(tAfter).isHistory === false

    db.since(tBefore).dbName === "hello"
    db.since(tBefore).basisT === tAfter
    db.since(tBefore).asOfT === 0
    db.since(tBefore).sinceT === tBefore
    db.since(tBefore).isHistory === false

    db.since(tAfter).dbName === "hello"
    db.since(tAfter).basisT === tAfter
    db.since(tAfter).asOfT === 0
    db.since(tAfter).sinceT === tAfter
    db.since(tAfter).isHistory === false

    db.history.dbName === "hello"
    db.history.basisT === tAfter
    db.history.asOfT === 0
    db.history.sinceT === 0
    db.history.isHistory === true
  }


  "as-of" in new AsyncSetup {

    // Current state
    films(conn.db) === threeFilms

    // State before last tx
    films(conn.db.asOf(tBefore)) === Nil

    // State after last tx same as current state
    films(conn.db.asOf(tAfter)) === threeFilms

    // We can use the transaction id too
    films(conn.db.asOf(txIdBefore)) === Nil
  }


  "since" in new AsyncSetup {
    // State created since previous t
    films(conn.db.since(tBefore)) === threeFilms

    // Nothing created after
    films(conn.db.since(tAfter)) === Nil
  }


  "with" in new AsyncSetup {
    val wDb = waitFor(conn.withDb).toOption.get
    val db = conn.db

    // Updated `with` db value
    val wDb2 = waitFor(db.`with`(wDb, film4)).toOption.get

    films(wDb2) === fourFilms

    // Add more data to `wDb2`
    val wDb3 = waitFor(db.`with`(wDb2.datomicDb, film5)).toOption.get
    films(wDb3) === (fourFilms :+ "Film 5").sorted

    // Current state is unaffected
    films(conn.db) === threeFilms
  }


  "with - single invocation" in new AsyncSetup {
    // As a convenience, a single-invocation shorter version of `with`:
    films(waitFor(conn.widh(film4)).toOption.get) === fourFilms

    // Applying another data set still augments the original db
    films(waitFor(conn.widh(film5)).toOption.get) === (threeFilms :+ "Film 5").sorted

    // Current state is unaffected
    films(conn.db) === threeFilms
  }


  "history" in new AsyncSetup {

    // Not testing Peer Server history since history is accumulating when we
    // can't re-create database for each test without shutting down Peer Server.

    if (isDevLocal) {

      // Current and history db are currently the same
      films(conn.db) == threeFilms
      films(conn.db.history) == threeFilms

      // As long as we only add data, current/history will be the same
      val tx = waitFor(conn.transact(film4)).toOption.get
      films(conn.db) == fourFilms
      films(conn.db.history) == fourFilms

      // Now retract the last entity
      val retractedEid = tx.txData.toScala(List).last.e
      waitFor(conn.transact(list(list(read(":db/retractEntity"), retractedEid))))
      films(conn.db) == threeFilms
      films(conn.db.history) == fourFilms

      // History of movie title assertions and retractions
      waitFor(AsyncDatomic.q(
        """[:find ?movie-title ?tx ?added
          |:where [_ :movie/title ?movie-title ?tx ?added]]""".stripMargin,

        // Use history database
        conn.db.history
      )).head.toOption.get.iterator().asScala.toList
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


  "datoms" in new AsyncSetup {
    waitFor(conn.db.datoms(
      ":avet",
      list(read(":movie/title"))
    )).toOption.get.toScala(List).map(_.v.toString).sorted === threeFilms
  }


  "indexRange" in new AsyncSetup {
    waitFor(conn.db.indexRange(":movie/title")).toOption.get.toScala(List).sortBy(_.e) === List(
      Datom(e1, a1, "The Goonies", txIdAfter, true),
      Datom(e2, a1, "Commando", txIdAfter, true),
      Datom(e3, a1, "Repo Man", txIdAfter, true),
    )
  }


  "pull" in new AsyncSetup {
    waitFor(conn.db.pull("[*]", eid)).toOption.get.toString ===
      s"""{:db/id $eid, :movie/title "Repo Man", :movie/genre "punk dystopia", :movie/release-year 1984}"""

    waitFor(conn.db.pull("[*]", eid, 1000)).toOption.get.toString ===
      s"""{:db/id $eid, :movie/title "Repo Man", :movie/genre "punk dystopia", :movie/release-year 1984}"""

    // dev-local in-memory db will pull within 1 ms
    if (!isDevLocal)
      waitFor(conn.db.pull("[*]", eid, 1)).toOption.get must throwA(
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


  "indexPull" in new AsyncSetup {

    // Pull lazy java Stream of indexes
    val javaStream: jStream[_] = waitFor(conn.db.indexPull(
      ":avet",
      "[:movie/title :movie/release-year]",
      "[:movie/release-year 1985]"
    )).toOption.get

    // Lazily convert to scala LazyList
    val scalaLazyList: LazyList[Any] = javaStream.toScala(LazyList)

    // Evaluate lazy list
    scalaLazyList.size === 2

    // LazyList in Scala can be accessed multiple times contrary to java Stream
    // that can only be consumed once (like an Iterator).
    val firstFilm = scalaLazyList.head

    // The underlying type of pulled indexes are clojure.lang.PersistentArrayMaps
    // that need to be cast to java Maps before we can be access values of the map.
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