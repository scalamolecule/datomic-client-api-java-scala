package datomicJavaScala

import java.util.{List => jList}
import datomic.Util
import datomic.Util.{list, read}

trait SchemaAndData {

  // Dev-local is automatically indexing all attribute values (can't add index option)
  val title = Util.map(
    Util.read(":db/ident"), Util.read(":movie/title"),
    Util.read(":db/valueType"), Util.read(":db.type/string"),
    Util.read(":db/cardinality"), Util.read(":db.cardinality/one"),
    Util.read(":db/doc"), "The title of the movie",
  )

  // Peer Server needs indexing to allow :avet look-ups
  val titleIndexed = Util.map(
    Util.read(":db/ident"), Util.read(":movie/title"),
    Util.read(":db/valueType"), Util.read(":db.type/string"),
    Util.read(":db/cardinality"), Util.read(":db.cardinality/one"),
    Util.read(":db/doc"), "The title of the movie",
    Util.read(":db/index"), true.asInstanceOf[Object],
  )

  def schema(addIndex: Boolean): jList[_] = Util.list(
    if (addIndex) titleIndexed else title,
    Util.map(
      Util.read(":db/ident"), Util.read(":movie/genre"),
      Util.read(":db/valueType"), Util.read(":db.type/string"),
      Util.read(":db/cardinality"), Util.read(":db.cardinality/one"),
      Util.read(":db/doc"), "The genre of the movie",
    ),
    Util.map(
      Util.read(":db/ident"), Util.read(":movie/release-year"),
      Util.read(":db/valueType"), Util.read(":db.type/long"),
      Util.read(":db/cardinality"), Util.read(":db.cardinality/one"),
      Util.read(":db/doc"), "The year the movie was released in theaters"
    )
  )

  val data: jList[_] = Util.list(
    Util.map(
      Util.read(":movie/title"), "The Goonies",
      Util.read(":movie/genre"), "action/adventure",
      Util.read(":movie/release-year"), 1985,
    ),
    Util.map(
      Util.read(":movie/title"), "Commando",
      Util.read(":movie/genre"), "thriller/action",
      Util.read(":movie/release-year"), 1985
    ),
    Util.map(
      Util.read(":movie/title"), "Repo Man",
      Util.read(":movie/genre"), "punk dystopia",
      Util.read(":movie/release-year"), 1984
    )
  )

  val filmQuery =
    """[:find ?movie-title
      |:where [_ :movie/title ?movie-title]]""".stripMargin


  val threeFilms = List("Commando", "Repo Man", "The Goonies")

  val film4     = list(Util.map(read(":movie/title"), "Film 4"))
  val film5     = list(Util.map(read(":movie/title"), "Film 5"))

  val fourFilms = (threeFilms :+ "Film 4").sorted
}
