package datomicJavaScala.client.api.sync

import java.util.{List => jList}
import datomic.Util.list
import datomicJavaScala.SchemaAndData
import org.specs2.mutable.Specification
import org.specs2.specification.core.{Fragments, Text}

/*
Multiple-environment setup inspiration:
https://github.com/etorreborre/specs2/issues/827
*/

trait SetupSpec extends Specification with SchemaAndData {

//    override def map(fs: => Fragments): Fragments =
//      step(setupDevLocal()) ^
//        fs.mapDescription(d => Text(s"${ClientProvider.system}: " + d.show))

  override def map(fs: => Fragments): Fragments =
    step(setupDevLocal()) ^
//      fs.mapDescription(d => Text(s"${ClientProvider.system}: " + d.show))
      fs.mapDescription(d => Text(s"${ClientProvider.system}: " + d.show)) ^
      step(setupPeerServer()) ^
      fs.mapDescription(d => Text(s"${ClientProvider.system}: " + d.show))


  def setupDevLocal(): Unit = {
    val client = Datomic.clientForDevLocal("dev")

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