package datomicJavaScala.client.api.async

import java.util.{List => jList}
import datomic.Util.list
import datomicJavaScala.SchemaAndData
import datomicJavaScala.util.ClojureBridge
import org.specs2.mutable.Specification
import org.specs2.specification.core.{Fragments, Text}
import scala.jdk.CollectionConverters._

/*
Multiple-environment setup inspiration:
https://github.com/etorreborre/specs2/issues/827
*/

trait AsyncSetupSpec extends Specification with SchemaAndData with ClojureBridge {


  // todo: awaiting to find a way to invoke data as maps against Peer Server
    override def map(fs: => Fragments): Fragments =
      step(setupDevLocal()) ^
        fs.mapDescription(d => Text(s"${AsyncClientProvider.system}: " + d.show))
//        fs.mapDescription(d => Text(s"${AsyncClientProvider.system}: " + d.show)) ^
//        step(setupPeerServer()) ^
//        fs.mapDescription(d => Text(s"${AsyncClientProvider.system}: " + d.show))


  def setupDevLocal(): Unit = {
    val client: AsyncClient = AsyncDatomic.clientForDevLocal("free")

    // Re-create db
    client.deleteDatabase("hello").realize
    client.deleteDatabase("world").realize
    client.createDatabase("hello").realize
    val conn: AsyncConnection = client.connect("hello")
    conn.transact(schema(false)).realize

    AsyncClientProvider.system = "dev-local"
    AsyncClientProvider.client = client
    AsyncClientProvider.conn = conn
    AsyncClientProvider.txReport = conn.transact(data).realize
  }


  def setupPeerServer(): Unit = {
    val client = AsyncDatomic.clientForPeerServer("myaccesskey", "mysecret", "localhost:8998")

    // Using the db associated with the Peer Server connection
    val conn = client.connect("hello")

    // Install schema if necessary
    if (AsyncDatomic.q(
      "[:find ?e :where [?e :db/ident :movie/title]]",
      conn.db
    ).realize.toString == "[]") {
      println("Installing Peer Server hello db schema...")
      conn.transact(schema(true)).realize
    }

    // Retract current data
    AsyncDatomic.q("[:find ?e :where [?e :movie/title _]]", conn.db).realize
      .asInstanceOf[jList[_]].forEach { l =>
      val eid: Any = l.asInstanceOf[jList[_]].get(0)
      conn.transact(list(list(":db/retractEntity", eid))).realize
    }

    AsyncClientProvider.system = "peer-server"
    AsyncClientProvider.client = client
    AsyncClientProvider.conn = conn
    AsyncClientProvider.txReport = conn.transact(data).realize
  }
}