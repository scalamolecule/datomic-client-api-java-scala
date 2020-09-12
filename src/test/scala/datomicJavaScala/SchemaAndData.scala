package datomicJavaScala

import java.util.{List => jList}
import datomic.Util
import datomic.Util._

trait SchemaAndData {

  // Dev-local is automatically indexing all attribute values (can't add index option)
  val title = Util.map(
    read(":db/ident"), read(":movie/title"),
    read(":db/valueType"), read(":db.type/string"),
    read(":db/cardinality"), read(":db.cardinality/one"),
    read(":db/doc"), "The title of the movie",
  )

  // Peer Server needs indexing to allow :avet look-ups
  val titleIndexed = Util.map(
    read(":db/ident"), read(":movie/title"),
    read(":db/valueType"), read(":db.type/string"),
    read(":db/cardinality"), read(":db.cardinality/one"),
    read(":db/doc"), "The title of the movie",
    read(":db/index"), true.asInstanceOf[Object],
  )

  def schema(addIndex: Boolean): jList[_] = list(
    if (addIndex) titleIndexed else title,
    Util.map(
      read(":db/ident"), read(":movie/genre"),
      read(":db/valueType"), read(":db.type/string"),
      read(":db/cardinality"), read(":db.cardinality/one"),
      read(":db/doc"), "The genre of the movie",
    ),
    Util.map(
      read(":db/ident"), read(":movie/release-year"),
      read(":db/valueType"), read(":db.type/long"),
      read(":db/cardinality"), read(":db.cardinality/one"),
      read(":db/doc"), "The year the movie was released in theaters"
    )
  )

  val data: jList[_] = list(
    Util.map(
      read(":movie/title"), "The Goonies",
      read(":movie/genre"), "action/adventure",
      read(":movie/release-year"), 1985,
    ),
    Util.map(
      read(":movie/title"), "Commando",
      read(":movie/genre"), "thriller/action",
      read(":movie/release-year"), 1985
    ),
    Util.map(
      read(":movie/title"), "Repo Man",
      read(":movie/genre"), "punk dystopia",
      read(":movie/release-year"), 1984
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
