package datomicScala.client.api.sync

import datomic.Util
import datomic.Util._
import datomicScala.anomaly.{Forbidden, NotFound}


class DatomicTest extends SetupSpec {
  sequential


  "create client" >> {

    ClientProvider.system match {
      case "cloud" =>
        // with AWSCredentialsProviderChain
        // Uncomment and test if a cloud system is available
        // import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
        //    validClient(Datomic.clientForCloud(
        //      "us-east-1",
        //      "mysystem",
        //      "http://entry.us-east-1.mysystem.datomic.net:8182/",
        //      DefaultAWSCredentialsProviderChain.getInstance(),
        //      8182
        //    ))

        // with credentials profile name
        // Uncomment and test if a cloud system is available
        //    validClient(Datomic.clientForCloud(
        //      "us-east-1",
        //      "mysystem",
        //      "http://entry.us-east-1.mysystem.datomic.net:8182/",
        //      "myprofile",
        //      8182
        //    ))
        ok


      case "dev-local" =>
        // Install dev-local (https://docs.datomic.com/cloud/dev-local.html)
        // > mkdir ~/.datomic
        // > touch ~/.datomic/dev-local.edn
        // > open ~/.datomic/dev-local.edn
        // add path to where you want to save data as per instructions in link above

        // Add dependency to dev-local in your project
        // "com.datomic" % "dev-local" % "0.9.195",

        // As long dev-local has a dependency on clojure 1.10.0-alpha4
        // we also need to import a newer version of clojure
        // "org.clojure" % "clojure" % "1.10.1",

        // (No need to start a transactor)

        // Retrieve client for a specific system
        // (this one has been created in SetupSpec)
        val client: Client = Datomic.clientForDevLocal("Hello system name")

        // Confirm that client is valid and can connect to a database
        client.connect("hello")

        // System name 'x' has not been established
        Datomic.clientForDevLocal("x").connect("hello") must throwA(
          NotFound("Db not found: hello")
        )


      case "peer-server" =>
        /*
          To run tests against a Peer Server do these 3 steps first:

          1. Start transactor
          > bin/transactor config/samples/free-transactor-template.properties

          2. Create sample db 'hello' by running 'create hello db' test (only) in CreateTestDb
          Peer.createDatabase("datomic:free://localhost:4334/hello")

          Start Peer Server for some existing database (like `hello` here)
          > bin/run -m datomic.peer-server -h localhost -p 8998 -a myaccesskey,mysecret -d hello,datomic:dev://localhost:4334/hello
         */

        val client: Client =
          Datomic.clientForPeerServer("myaccesskey", "mysecret", "localhost:8998")

        // Confirm that client is valid and can connect to a database
        client.connect("hello")

        // Note that a Client is returned immediately without contacting
        // a server and can thus be invalid.
        val client2: Client =
          Datomic.clientForPeerServer("admin", "nice-try", "localhost:8998")

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
        Datomic.clientForPeerServer("myaccesskey", "mysecret", "x")
          .connect("hello") must throwA(
          NotFound("x: nodename nor servname provided, or not known")
        )
    }
  }


  "q" in new Setup {
    // query & args / String
    Datomic.q(
      """[:find ?movie-title
        |:where [_ :movie/title ?movie-title]]""".stripMargin,
      conn.db
    ) === list(list("Commando"), list("The Goonies"), list("Repo Man"))

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
