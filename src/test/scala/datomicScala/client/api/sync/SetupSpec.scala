package datomicScala.client.api.sync

import java.util.{List => jList}
import datomic.Util.list
import datomicScala.SchemaAndData
import org.specs2.mutable.Specification
import org.specs2.specification.core.{Fragments, Text}


trait SetupSpec extends Specification with SchemaAndData {


  override def map(fs: => Fragments): Fragments =
    step(setupDevLocal()) ^
      fs.mapDescription(d => Text(s"${ClientProvider.system}: " + d.show)) ^
      step(setupPeerServer()) ^
      fs.mapDescription(d => Text(s"${ClientProvider.system}: " + d.show))


  def setupDevLocal(): Unit = {
    val client = Datomic.clientForDevLocal("Hello system name")

    // Re-create db
    client.deleteDatabase("hello")
    client.deleteDatabase("world")
    client.createDatabase("hello")
    val conn: Connection = client.connect("hello")
    conn.transact(schema(false))

    ClientProvider.system = "dev-local"
    ClientProvider.client = client
    ClientProvider.conn = conn
    ClientProvider.txReport = conn.transact(data)
  }


  def setupPeerServer(): Unit = {
    val client = Datomic.clientForPeerServer("myaccesskey", "mysecret", "localhost:8998")

    // Using the db associated with the Peer Server connection
    val conn = client.connect("hello")

    // Install schema if necessary
    if (Datomic.q(
      "[:find ?e :where [?e :db/ident :movie/title]]",
      conn.db
    ).toString == "[]") {
      println("Installing Peer Server hello db schema...")
      conn.transact(schema(true))
    }

    // Retract current data
    Datomic.q("[:find ?e :where [?e :movie/title _]]", conn.db)
      .asInstanceOf[jList[_]].forEach { l =>
      val eid: Any = l.asInstanceOf[jList[_]].get(0)
      conn.transact(list(list(":db/retractEntity", eid)))
    }

    ClientProvider.system = "peer-server"
    ClientProvider.client = client
    ClientProvider.conn = conn
    ClientProvider.txReport = conn.transact(data)
  }
}