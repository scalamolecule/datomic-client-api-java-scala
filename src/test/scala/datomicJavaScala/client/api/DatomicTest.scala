package datomicJavaScala.client.api

import datomic.Util
import datomic.Util._
import datomic.client.api.protocols
import datomicJavaScala.{ClientProvider, SetupSpec, Setup}
import org.specs2.matcher.EqualityMatcher
import scala.jdk.StreamConverters._


class DatomicTest extends SetupSpec {
  sequential

  def validClient(client: Client): EqualityMatcher[protocols.Client] =
    typedEqualTo[datomic.client.api.protocols.Client](client.datomicClient)


//  "create client" >> {
//
//    ClientProvider.system match {
//      case "cloud" =>
//        // with AWSCredentialsProviderChain
//        // Uncomment and test if a cloud system is available
//        // import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
//        //    validClient(Datomic.clientForCloud(
//        //      "us-east-1",
//        //      "mysystem",
//        //      "http://entry.us-east-1.mysystem.datomic.net:8182/",
//        //      DefaultAWSCredentialsProviderChain.getInstance(),
//        //      8182
//        //    ))
//
//        // with credentials profile name
//        // Uncomment and test if a cloud system is available
//        //    validClient(Datomic.clientForCloud(
//        //      "us-east-1",
//        //      "mysystem",
//        //      "http://entry.us-east-1.mysystem.datomic.net:8182/",
//        //      "myprofile",
//        //      8182
//        //    ))
//        ok
//
//      case "dev-local" =>
//        // Install dev-local (https://docs.datomic.com/cloud/dev-local.html)
//        // > mkdir ~/.datomic
//        // > touch ~/.datomic/dev-local.edn
//        // > open ~/.datomic/dev-local.edn
//        // add path to where you want to save data as per instructions in link above
//
//        // Add dependency to dev-local in your project
//        // "com.datomic" % "dev-local" % "0.9.195",
//
//        // As long dev-local has a dependency on clojure 1.10.0-alpha4
//        // we also need to import a newer version of clojure
//        // "org.clojure" % "clojure" % "1.10.1",
//
//        // (No need to start a transactor)
//
//        val client: Client = Datomic.clientForDevLocal("free")
//        validClient(client)
//
//      case "peer-server" =>
//        /*
//          To run tests against a Peer Server do these 3 steps first:
//
//          1. Start transactor
//          > bin/transactor config/samples/free-transactor-template.properties
//
//          2. Create sample db 'hello' by running 'create hello db' test (only) in CreateTestDb
//          Peer.createDatabase("datomic:free://localhost:4334/hello")
//
//          Start Peer Server for some existing database (like `hello` here)
//          > bin/run -m datomic.peer-server -h localhost -p 8998 -a myaccesskey,mysecret -d hello,datomic:dev://localhost:4334/hello
//         */
//
//        val client: Client = Datomic.clientForPeerServer("myaccesskey", "mysecret", "localhost:8998")
//        validClient(client)
//    }
//    ok
//  }
//
//
//  "q query & args / String" in new Setup {
//    Datomic.q(
//      """[:find ?movie-title
//        |:where [_ :movie/title ?movie-title]]""".stripMargin,
//      conn.db
//    ).toString === """[["Commando"] ["The Goonies"] ["Repo Man"]]"""
//  }
//
//  "q query & args / data structure" in new Setup {
//    Datomic.q(
//      list(
//        read(":find"), read("?title"),
//        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
//      ),
//      conn.db
//    ).toString === """[["Commando"] ["The Goonies"] ["Repo Man"]]"""
//  }
//
//  "q arg-map / String" in new Setup {
//    Datomic.q(
//      Util.map(
//        read(":query"),
//        """[:find ?movie-title
//          |:where [_ :movie/title ?movie-title]]""".stripMargin,
//        read(":args"), list(conn.db.datomicDb),
//      )
//    ).toString === """[["Commando"] ["The Goonies"] ["Repo Man"]]"""
//  }
//
//  "q arg-map / data structure" in new Setup {
//    Datomic.q(
//      Util.map(
//        read(":query"), list(
//          read(":find"), read("?title"),
//          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
//        ),
//        read(":args"), list(conn.db.datomicDb)
//      )
//    ).toString === """[["Commando"] ["The Goonies"] ["Repo Man"]]"""
//  }
//
//  "q arg-map / String with :limit" in new Setup {
//    Datomic.q(
//      Util.map(
//        read(":query"),
//        """[:find ?movie-title
//          |:where [_ :movie/title ?movie-title]]""".stripMargin,
//        read(":args"), list(conn.db.datomicDb),
//        read(":limit"), 2,
//      )
//    ).toString === """[["Commando"] ["The Goonies"]]"""
//  }
//
//  "q arg-map / String with :offset, :limit :timeout" in new Setup {
//    Datomic.q(
//      Util.map(
//        read(":query"),
//        """[:find ?movie-title
//          |:where [_ :movie/title ?movie-title]]""".stripMargin,
//        read(":args"), list(conn.db.datomicDb),
//        read(":offset"), 1,
//        read(":limit"), 1,
//        read(":timeout"), 2000,
//      )
//    ).toString === """[["The Goonies"]]"""
//  }


  "qseq / String" in new Setup {
    Datomic.qseq(
      """[:find ?movie-title
        |:where [_ :movie/title ?movie-title]]""".stripMargin,
      conn.db
    ).toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )
  }

  "qseq query & args / data structure" in new Setup {
    Datomic.qseq(
      list(
        read(":find"), read("?title"),
        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
      ),
      conn.db
    ).toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )
  }

  "qseq arg-map / String" in new Setup {
    Datomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
      )
    ).toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )
  }

  "qseq arg-map / data structure" in new Setup {
    Datomic.qseq(
      Util.map(
        read(":query"), list(
          read(":find"), read("?title"),
          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
        ),
        read(":args"), list(conn.db.datomicDb)
      )
    ).toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )
  }

  "qseq arg-map / String with :limit" in new Setup {
    Datomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":limit"), 2,
      )
    ).toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"),
    )
  }

  "qseq arg-map / String with :offset, :limit :timeout" in new Setup {
    Datomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":offset"), 1,
        read(":limit"), 1,
        read(":timeout"), 2000,
      )
    ).toScala(LazyList) === LazyList(
      list("The Goonies"),
    )
  }
}
