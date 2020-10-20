package datomicScala.client.api.async

import java.util.stream
import java.util.stream.{Stream => jStream}
import cats.effect.IO
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import datomic.Util
import datomic.Util.{list, _}
import datomicScala.{CognitectAnomaly, Forbidden, NotFound, SpecAsync}
import scala.concurrent.Future
import scala.jdk.StreamConverters._


class AsyncDatomicTest extends SpecAsync {
  sequential


  "create client" >> {
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
        waitFor(AsyncDatomic.clientDevLocal("x").connect("hello")) === Left(
          NotFound("Db not found: hello")
        )

        // Wrong db name
        waitFor(AsyncDatomic.clientDevLocal("Hello system name").connect("y")) === Left(
          NotFound("Db not found: y")
        )
      }

      case "peer-server" => {
        // TODO: async peer server not available before bug fix
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
          .connect("hello") === Left(
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


  // fs2 collides with specs2, so we need to call this outside the test body
  def fs2StreamOfChunks(db: AsyncDb, chunkSize: Int)
  : fs2.Stream[IO, Either[CognitectAnomaly, jStream[_]]] = {
    AsyncDatomic.qStream(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(db.datomicDb),
        read(":chunk"), chunkSize,
      )
    )
  }

  // db will only be available inside test
  val streamOfChunksOf1: AsyncDb => fs2.Stream[IO, Either[CognitectAnomaly, jStream[_]]] =
    (db: AsyncDb) => fs2StreamOfChunks(db, 1)


  "q" in new AsyncSetup {

    // query & args / String - with intermediary type resolutions
    val futureLazyChunks: Future[LazyList[Either[CognitectAnomaly, jStream[_]]]] =
      AsyncDatomic.q(
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        conn.db
      )
    val lazyChunks: LazyList[Either[CognitectAnomaly, jStream[_]]] = waitFor(futureLazyChunks)
    val firstChunk: Either[CognitectAnomaly, jStream[_]] = lazyChunks.head
    val chunkDataOptional: Option[jStream[_]] = firstChunk.toOption
    val chunkData: jStream[_] = chunkDataOptional.get
    val chunkDataScala: List[Any] = chunkData.toScala(List)
    chunkDataScala === List(
      list("Commando"), list("The Goonies"), list("Repo Man")
    )

    // Input arg(s)
    waitFor(AsyncDatomic.q(
      """[:find ?movie-title
        |:in $ ?year
        |:where [?e :movie/release-year ?year]
        |       [?e :movie/title ?movie-title]
        |]""".stripMargin,
      conn.db, 1984
    )).head.toOption.get.toScala(List) === List(
      list("Repo Man")
    )

    waitFor(AsyncDatomic.q(
      """[:find ?movie-title
        |:in $ ?year
        |:where [?e :movie/release-year ?year]
        |       [?e :movie/title ?movie-title]
        |]""".stripMargin,
      conn.db, 1984
    )).head.toOption.get.toScala(List) === List(
      list("Repo Man")
    )

    // query & args / data structure
    waitFor(AsyncDatomic.q(
      list(
        read(":find"), read("?title"),
        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
      ),
      conn.db
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"), list("Repo Man")
    )

    // arg-map / String
    waitFor(AsyncDatomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
      )
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"), list("Repo Man")
    )

    // arg-map / data structure
    waitFor(AsyncDatomic.q(
      Util.map(
        read(":query"), list(
          read(":find"), read("?title"),
          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
        ),
        read(":args"), list(conn.db.datomicDb)
      )
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"), list("Repo Man")
    )

    // arg-map / String with :limit
    waitFor(AsyncDatomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":limit"), 2,
      )
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies")
    )

    // arg-map / String with :offset, :limit :timeout
    waitFor(AsyncDatomic.q(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":offset"), 1,
        read(":limit"), 1,
        read(":timeout"), 2000,
      )
    )).head.toOption.get.toScala(List) === List(
      list("The Goonies")
    )

    // Chunks
    def chunksOf(chunkSize: Int): LazyList[Either[CognitectAnomaly, stream.Stream[_]]] = {
      waitFor(AsyncDatomic.q(
        Util.map(
          read(":query"),
          """[:find ?movie-title
            |:where [_ :movie/title ?movie-title]]""".stripMargin,
          read(":args"), list(conn.db.datomicDb),
          read(":chunk"), chunkSize,
        )
      ))
    }

    // Retrieve successive chunks

    // First chunk is fetched and memoized in LazyList
    val res1 = chunksOf(1)
    // Memoized first chunk retrieved
    res1(0).toOption.get.toScala(List) === List(list("Commando"))
    // Second chunk is fetched
    res1(1).toOption.get.toScala(List) === List(list("The Goonies"))
    // Third chunk is fetched
    res1(2).toOption.get.toScala(List) === List(list("Repo Man"))
    // Empty tail is also evaluated to calculate size
    res1.size === 3

    val res2 = chunksOf(2)
    res2(0).toOption.get.toScala(List) === List(list("Commando"), list("The Goonies"))
    res2(1).toOption.get.toScala(List) === List(list("Repo Man"))
    res2.size === 2

    val res3 = chunksOf(3)
    res3(0).toOption.get.toScala(List) ===
      List(list("Commando"), list("The Goonies"), list("Repo Man"))
    res3.size === 1


    // Alternatively get chunk from fs2 Stream (example implementation)
    // This allows the first chunk to be lazy also.
    // Usage is flexible and follows pure fp principles but is verbose too:

    // Create Stream
    streamOfChunksOf1(conn.db)
      // Get first chunk Stream
      .head
      .compile
      .toList
      // evaluate first chunk (call <!!)
      .unsafeRunSync()
      // get chunk
      .head
      // Convert Right value to Option
      .toOption
      // Get java Stream of data
      .get
      .toScala(List) ===
      List(list("Commando"))
  }


  // qseq since 1.0.6165

  "qseq" in new AsyncSetup {

    // query & args / String
    waitFor(AsyncDatomic.qseq(
      """[:find ?movie-title
        |:where [_ :movie/title ?movie-title]]""".stripMargin,
      conn.db
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // Input arg(s)
    waitFor(AsyncDatomic.qseq(
      """[:find ?movie-title
        |:in $ ?year
        |:where [?e :movie/release-year ?year]
        |       [?e :movie/title ?movie-title]
        |]""".stripMargin,
      conn.db, 1984
    )).head.toOption.get.toScala(List) === List(
      list("Repo Man")
    )

    // qseq query & args / data structure
    waitFor(AsyncDatomic.qseq(
      list(
        read(":find"), read("?title"),
        read(":where"), list(read("_"), read(":movie/title"), read("?title"))
      ),
      conn.db
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // qseq arg-map / String
    waitFor(AsyncDatomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
      )
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // arg-map / data structure
    waitFor(AsyncDatomic.qseq(
      Util.map(
        read(":query"), list(
          read(":find"), read("?title"),
          read(":where"), list(read("_"), read(":movie/title"), read("?title"))
        ),
        read(":args"), list(conn.db.datomicDb)
      )
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"), list("Repo Man"),
    )

    // arg-map / String with :limit
    waitFor(AsyncDatomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":limit"), 2,
      )
    )).head.toOption.get.toScala(List) === List(
      list("Commando"), list("The Goonies"),
    )

    // arg-map / String with :offset, :limit :timeout
    waitFor(AsyncDatomic.qseq(
      Util.map(
        read(":query"),
        """[:find ?movie-title
          |:where [_ :movie/title ?movie-title]]""".stripMargin,
        read(":args"), list(conn.db.datomicDb),
        read(":offset"), 1,
        read(":limit"), 1,
        read(":timeout"), 2000,
      )
    )).head.toOption.get.toScala(List) === List(
      list("The Goonies"),
    )
  }
}
