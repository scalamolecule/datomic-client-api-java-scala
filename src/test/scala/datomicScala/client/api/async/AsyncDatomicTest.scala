package datomicScala.client.api.async

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import datomic.Util
import datomic.Util.{list, _}
import datomicScala.{Forbidden, NotFound, SpecAsync}
import scala.jdk.StreamConverters._


class AsyncDatomicTest extends SpecAsync {
  sequential


  "create client" >> {

    // Not much of a test really - just checking that we can produce some clients

    system match {
      case "dev-local" => {
        /*
          Install dev-local (https://docs.datomic.com/cloud/dev-local.html)
          > mkdir ~/.datomic
          > touch ~/.datomic/dev-local.edn
          > open ~/.datomic/dev-local.edn
          add path to where you want to save data as per instructions in link above

          Add dependency to dev-local in your project
          "com.datomic" % "dev-local" % "0.9.195",

          As long dev-local has a dependency on clojure 1.10.0-alpha4
          we also need to import a newer version of clojure
          "org.clojure" % "clojure" % "1.10.1",

          (No need to start a transactor)
         */

        // Retrieve client for a specific system
        // (this one has been created in SetupSpec)
        val client: AsyncClient = AsyncDatomic.clientDevLocal("Hello system name")

        // Confirm that client is valid and can connect to a database
        client.connect("hello")

        // Wrong system name
        AsyncDatomic.clientDevLocal("x").connect("hello") must throwA(
          NotFound("Db not found: hello")
        )

        // Wrong db name
        AsyncDatomic.clientDevLocal("Hello system name").connect("y") must throwA(
          NotFound("Db not found: y")
        )
      }

      case "peer-server" => {
        /*
          To run tests against a Peer Server do these 3 steps first:

          1. Start transactor
          > bin/transactor config/samples/free-transactor-template.properties

          2. Create sample db 'hello' by running 'create hello db' test (only) in CreateTestDb
          Peer.createDatabase("datomic:free://localhost:4334/hello")

          Start Peer Server for some existing database (like `hello` here)
          > bin/run -m datomic.peer-server -h localhost -p 8998 -a myaccesskey,mysecret -d hello,datomic:dev://localhost:4334/hello
         */

        val client: AsyncClient =
          AsyncDatomic.clientPeerServer("myaccesskey", "mysecret", "localhost:8998")

        // Confirm that client is valid and can connect to a database
        client.connect("hello")

        // Note that a Client is returned immediately without contacting
        // a server and can thus be invalid.
        val client2: AsyncClient =
          AsyncDatomic.clientPeerServer("admin", "nice-try", "localhost:8998")

        // Invalid setup shows on first call to server
        try {
          client2.connect("hello")
        } catch {
          case forbidden: Forbidden =>
            forbidden.msg === "forbidden"
            forbidden.httpRequest("status") === 403
            forbidden.httpRequest("body") === null

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
        AsyncDatomic.clientPeerServer("myaccesskey", "mysecret", "x")
          .connect("hello") must throwA(
          NotFound("x: nodename nor servname provided, or not known")
        )
      }

      case "cloud" => {
        val client1: AsyncClient = AsyncDatomic.clientCloud(
          "us-east-1",
          "mysystem",
          "http://entry.us-east-1.mysystem.datomic.net:8182/",
          DefaultAWSCredentialsProviderChain.getInstance(),
          8182
        )
        // todo: test against a live cloud client

        // with credentials profile name
        // Uncomment and test if a cloud system is available
        val client2: AsyncClient = AsyncDatomic.clientCloud(
          "us-east-1",
          "mysystem",
          "http://entry.us-east-1.mysystem.datomic.net:8182/",
          "myprofile",
          8182
        )
        // todo: test against a live cloud client
      }
    }
    ok
  }


  "q" in new AsyncSetup {

    // query & args / String
    AsyncDatomic.q(
      """[:find ?movie-title
        |:where [_ :movie/title ?movie-title]]""".stripMargin,
      conn.db
    ).realize.toScala(List) ===
      List(list("Commando"), list("The Goonies"), list("Repo Man"))

    // Input arg(s)
    AsyncDatomic.q(
      """[:find ?movie-title
        |:in $ ?year
        |:where [?e :movie/release-year ?year]
        |       [?e :movie/title ?movie-title]
        |]""".stripMargin,
      conn.db, 1984
    ).realize.toScala(List) === List(list("Repo Man"))

    // query & args / data structure
    AsyncDatomic.q(
      list(
        read(":find"), read("?title"),
        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
      ),
      conn.db
    ).realize.toScala(List) ===
      List(list("Commando"), list("The Goonies"), list("Repo Man"))

    // arg-map / String
    AsyncDatomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
      )
    ).realize.toScala(List) ===
      List(list("Commando"), list("The Goonies"), list("Repo Man"))

    // arg-map / data structure
    AsyncDatomic.q(
      Util.map(
        read(":query"), list(
          read(":find"), read("?title"),
          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
        ),
        read(":args"), list(conn.db.datomicDb)
      )
    ).realize.toScala(List) ===
      List(list("Commando"), list("The Goonies"), list("Repo Man"))

    // arg-map / String with :limit
    AsyncDatomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":limit"), 2,
      )
    ).realize.toScala(List) === List(list("Commando"), list("The Goonies"))

    // arg-map / String with :offset, :limit :timeout
    AsyncDatomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":offset"), 1,
        read(":limit"), 1,
        read(":timeout"), 2000,
      )
    ).realize.toScala(List) === List(list("The Goonies"))
  }


  // qseq since 1.0.6165

  "qseq" in new AsyncSetup {

    // query & args / String
    AsyncDatomic.qseq(
      """[:find ?movie-title
        |:where [_ :movie/title ?movie-title]]""".stripMargin,
      conn.db
    ).realize.toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // Input arg(s)
    AsyncDatomic.qseq(
      """[:find ?movie-title
        |:in $ ?year
        |:where [?e :movie/release-year ?year]
        |       [?e :movie/title ?movie-title]
        |]""".stripMargin,
      conn.db, 1984
    ).realize.toScala(List) === LazyList(list("Repo Man"))

    // qseq query & args / data structure
    AsyncDatomic.qseq(
      list(
        read(":find"), read("?title"),
        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
      ),
      conn.db
    ).realize.toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // qseq arg-map / String
    AsyncDatomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
      )
    ).realize.toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // arg-map / data structure
    AsyncDatomic.qseq(
      Util.map(
        read(":query"), list(
          read(":find"), read("?title"),
          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
        ),
        read(":args"), list(conn.db.datomicDb)
      )
    ).realize.toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // arg-map / String with :limit
    AsyncDatomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":limit"), 2,
      )
    ).realize.toScala(LazyList) === LazyList(
      list("Commando"), list("The Goonies"),
    )

    // arg-map / String with :offset, :limit :timeout
    AsyncDatomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":offset"), 1,
        read(":limit"), 1,
        read(":timeout"), 2000,
      )
    ).realize.toScala(LazyList) === LazyList(
      list("The Goonies"),
    )
  }
}
