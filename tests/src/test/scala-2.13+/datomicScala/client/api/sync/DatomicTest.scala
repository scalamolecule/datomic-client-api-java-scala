package datomicScala.client.api.sync

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import datomic.Peer.function
import datomic.Util
import datomic.Util._
import datomicClient.anomaly.{AnomalyWrapper, Forbidden, NotFound}
import datomicScala.Spec
import scala.util.control.NonFatal


class DatomicTest extends Spec with AnomalyWrapper {

  "create client" >> {
    system match {
      case "dev-local" =>
        /*
          Install dev-local (https://docs.datomic.com/cloud/dev-local.html)
          > mkdir ~/.datomic
          > touch ~/.datomic/dev-local.edn
          > open ~/.datomic/dev-local.edn
          add path to where you want to save data as per instructions in link above

          Add dependency to dev-local in your project
          "com.datomic" % "dev-local" % "1.0.238",

          As long dev-local has a dependency on clojure 1.10.0-alpha4
          we also need to import a newer version of clojure like this:
          "org.clojure" % "clojure" % "1.10.3"

          (No need to start a transactor)
         */

        // Confirm that client is valid and can connect to a database
        Datomic.clientDevLocal("test-datomic-client-api-scala-2.13").connect("hello")

        // Wrong system name
        Datomic.clientDevLocal("x").connect("hello") must throwA(
          NotFound("Db not found: hello")
        )

        // Wrong db name
        Datomic.clientDevLocal("test-datomic-client-api-scala-2.13").connect("y") must throwA(
          NotFound("Db not found: y")
        )


      case "peer-server" =>
        /*
          To run tests against a Peer Server do these 3 steps first:

          1. Start transactor
          > bin/transactor config/samples/dev-transactor-template.properties

          2. Create sample db 'hello' by running 'create hello db' test (only) in CreateTestDb

          Start Peer Server for some existing database (like `hello` here)
          > bin/run -m datomic.peer-server -a k,s -d hello,datomic:mem://hello
          or
          > bin/run -m datomic.peer-server -h localhost -p 8998 -a k,s -d hello,datomic:dev://localhost:4334/hello
         */

        // Confirm that client is valid and can connect to a database
        Datomic.clientPeerServer("k", "s", "localhost:8998").connect("hello")

        // Note that a Client is returned immediately without contacting
        // a server and can thus be invalid.
        val invalidClient: Client =
          Datomic.clientPeerServer("admin", "nice-try", "localhost:8998")

        try {
          invalidClient.connect("hello")
          throw new RuntimeException("Unexpectedly didn't throw a `Forbidden` exception")
        } catch {
          case forbidden: Forbidden =>
            forbidden.getMessage === "forbidden"
            forbidden.httpRequest.get("status") === 403
            forbidden.httpRequest.get("body") === null

          /*
          Example of forbidden.httpRequest data:

          Map(
            status -> 403,
            headers -> Map(
              server -> Jetty(9.3.7.v20160115),
              content-length -> 19,
              date -> Sun, 13 Sep 2020 19:14:36 GMT,
              content-type -> application/transit+msgpack
            ),
            body -> null
          )
          */
        }

        // Wrong endpoint
        try {
          Datomic.clientPeerServer("k", "s", "x").connect("hello")
          throw new RuntimeException("Unexpectedly didn't throw a `NotFound` exception")
        } catch {
          case notFound: NotFound =>
            // Is "x" or "x: nodename nor servname provided, or not known"
            notFound.msg.startsWith("x") === true
          case NonFatal(e)        =>
            throw new RuntimeException("Unexpectedly didn't throw a `NotFound` exception. Got " + e)
        }


      case "cloud" =>
        val client1: Client = Datomic.clientCloud(
          "us-east-1",
          "mysystem",
          "http://entry.us-east-1.mysystem.datomic.net:8182/",
          DefaultAWSCredentialsProviderChain.getInstance(),
          8182
        )
        // todo: test against a live cloud client

        // with credentials profile name
        // Uncomment and test if a cloud system is available
        val client2: Client = Datomic.clientCloud(
          "us-east-1",
          "mysystem",
          "http://entry.us-east-1.mysystem.datomic.net:8182/",
          "myprofile",
          8182
        )
      // todo: test against a live cloud client
    }
    ok
  }


  "q" in new Setup {
    // query & args / String
    Datomic.q(
      """[:find ?movie-title
        |:where [_ :movie/title ?movie-title]]""".stripMargin,
      conn.db
    ) === list(list("Commando"), list("The Goonies"), list("Repo Man"))

    // Input arg(s)
    Datomic.q(
      """[:find ?movie-title
        |:in $ ?year
        |:where [?e :movie/release-year ?year]
        |       [?e :movie/title ?movie-title]
        |]""".stripMargin,
      conn.db, 1984
    ) === list(list("Repo Man"))

    // query & args / data structure
    Datomic.q(
      list(
        read(":find"), read("?title"),
        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
      ),
      conn.db
    ) === list(list("Commando"), list("The Goonies"), list("Repo Man"))

    // arg-map / String
    Datomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
      )
    ) === list(list("Commando"), list("The Goonies"), list("Repo Man"))

    // arg-map / data structure
    Datomic.q(
      Util.map(
        read(":query"), list(
          read(":find"), read("?title"),
          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
        ),
        read(":args"), list(conn.db.datomicDb)
      )
    ) === list(list("Commando"), list("The Goonies"), list("Repo Man"))


    // arg-map / String with :limit
    Datomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":limit"), 2,
      )
    ) === list(list("Commando"), list("The Goonies"))

    // arg-map / String with :offset, :limit :timeout
    Datomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":offset"), 1,
        read(":limit"), 1,
        read(":timeout"), 2000
      )
    ) === list(list("The Goonies"))
  }


  // qseq since 1.0.6165

  "qseq" in new Setup {
    // query & args / String
    Datomic.qseq(
      """[:find ?movie-title
        |:where [_ :movie/title ?movie-title]]""".stripMargin,
      conn.db
    ) === LazyList(list("Commando"), list("The Goonies"), list("Repo Man"))


    // qseq query & args / data structure
    Datomic.qseq(
      list(
        read(":find"), read("?title"),
        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
      ),
      conn.db
    ) === LazyList(list("Commando"), list("The Goonies"), list("Repo Man"))

    // qseq arg-map / String
    Datomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
      )
    ) === LazyList(list("Commando"), list("The Goonies"), list("Repo Man"))

    // arg-map / data structure
    Datomic.qseq(
      Util.map(
        read(":query"), list(
          read(":find"), read("?title"),
          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
        ),
        read(":args"), list(conn.db.datomicDb)
      )
    ) === LazyList(list("Commando"), list("The Goonies"), list("Repo Man"))

    // arg-map / String with :limit
    Datomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":limit"), 2,
      )
    ) === LazyList(list("Commando"), list("The Goonies"))

    // arg-map / String with :offset, :limit :timeout
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
    ) === LazyList(list("The Goonies"))
  }
}
