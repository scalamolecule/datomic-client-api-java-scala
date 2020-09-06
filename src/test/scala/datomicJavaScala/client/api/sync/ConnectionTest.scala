package datomicJavaScala.client.api.sync

import datomicJavaScala.client.api.Datom
import datomicJavaScala.{SetupSpec, Setup}


class ConnectionTest extends SetupSpec {
  sequential


  "txRange" in new Setup {
    // For some reason we can't compare (7, Array(...)) but separately goes fine:
    val txRange = conn.txRange()
    txRange.last._1 === tAfter
    txRange.last._2 === Array(
      Datom(txIdAfter, 50, txInst, txIdAfter, true),
      Datom(e1, a1, "The Goonies", txIdAfter, true),
      Datom(e1, a2, "action/adventure", txIdAfter, true),
      Datom(e1, a3, 1985, txIdAfter, true),
      Datom(e2, a1, "Commando", txIdAfter, true),
      Datom(e2, a2, "thriller/action", txIdAfter, true),
      Datom(e2, a3, 1985, txIdAfter, true),
      Datom(e3, a1, "Repo Man", txIdAfter, true),
      Datom(e3, a2, "punk dystopia", txIdAfter, true),
      Datom(e3, a3, 1984, txIdAfter, true)
    )
  }


  // (withDb and widh are tested in DbTest...)


  "db" in new Setup {
    // Test if repeated calls do conn.db returns the samme db value (/object)
    val db = conn.db

    if (isDevLocal) {
      // Dev-local connection returns same database object
      db === conn.db
      assert(db == conn.db)

      // Value equality
      db.equals(conn.db)

      // Object identity equality
      db.eq(conn.db)

    } else {

      // Peer Server connection returns a database object copy
      db !== conn.db
      assert(db != conn.db)

      // Weirdly these test equal!?:

      // Value equality
      db.equals(conn.db)

      // Object identity equality
      db.eq(conn.db)
    }
  }

  "sync" in new Setup {

    // Db value the same
    conn.sync(tAfter).equals(dbAfter)

    // Db object identity
    if (isDevLocal) {
      // Same db object
      conn.sync(tAfter) === dbAfter
    } else {
      // Db object copy
      conn.sync(tAfter) !== dbAfter
    }
  }
}
