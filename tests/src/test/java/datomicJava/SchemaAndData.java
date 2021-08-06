package datomicJava;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.List;

import static datomic.Util.*;

public class SchemaAndData {

    public void println(String s) {
        System.out.println(s);
    }

    public static List<?> schemaDevLocal = list(
        map(
            read(":db/ident"), read(":movie/title"),
            read(":db/valueType"), read(":db.type/string"),
            read(":db/cardinality"), read(":db.cardinality/one"),
            read(":db/doc"), "The title of the movie"
        ),
        map(
            read(":db/ident"), read(":movie/genre"),
            read(":db/valueType"), read(":db.type/string"),
            read(":db/cardinality"), read(":db.cardinality/one"),
            read(":db/doc"), "The genre of the movie"
        ),
        map(
            read(":db/ident"), read(":movie/release-year"),
            read(":db/valueType"), read(":db.type/long"),
            read(":db/cardinality"), read(":db.cardinality/one"),
            read(":db/doc"), "The year the movie was released in theaters"
        )
    );

    public static List<?> schemaPeerServer = list(
        map(
            read(":db/ident"), read(":movie/title"),
            read(":db/valueType"), read(":db.type/string"),
            read(":db/cardinality"), read(":db.cardinality/one"),
            read(":db/doc"), "The title of the movie",
            read(":db/index"), true
        ),
        map(
            read(":db/ident"), read(":movie/genre"),
            read(":db/valueType"), read(":db.type/string"),
            read(":db/cardinality"), read(":db.cardinality/one"),
            read(":db/doc"), "The genre of the movie"
        ),
        map(
            read(":db/ident"), read(":movie/release-year"),
            read(":db/valueType"), read(":db.type/long"),
            read(":db/cardinality"), read(":db.cardinality/one"),
            read(":db/doc"), "The year the movie was released in theaters",
            read(":db/index"), true
        )
    );

    public static List<?> filmData = list(
        map(
            read(":movie/title"), "The Goonies",
            read(":movie/genre"), "action/adventure",
            read(":movie/release-year"), 1985
        ),
        map(
            read(":movie/title"), "Commando",
            read(":movie/genre"), "thriller/action",
            read(":movie/release-year"), 1985
        ),
        map(
            read(":movie/title"), "Repo Man",
            read(":movie/genre"), "punk dystopia",
            read(":movie/release-year"), 1984
        )
    );


    public static String filmQuery = "[:find ?movie-title :where [_ :movie/title ?movie-title]]";


    public static List<?> threeFilms = list("Commando", "Repo Man", "The Goonies");
    public static List<?> fourFilms = list("Commando", "Film 4", "Repo Man", "The Goonies");
    public static List<?> fiveFilms = list("Commando", "Film 4", "Film 5", "Repo Man", "The Goonies");
    public static List<?> sixFilms = list("Commando", "Film 4", "Film 5", "Film 6", "Repo Man", "The Goonies");

    public static List<?> film4 = list(map(read(":movie/title"), "Film 4"));
    public static List<?> film5 = list(map(read(":movie/title"), "Film 5"));
    public static List<?> film6 = list(map(read(":movie/title"), "Film 6"));
    public static List<?> film4and5 = list(
        map(read(":movie/title"), "Film 4"),
        map(read(":movie/title"), "Film 5")
    );


    // Test hacks to be able to read files with different base paths when testing in Intellij/sbt
    // todo: resolve in Intellij/sbt settings instead
    public FileReader getFileReader(String path) throws FileNotFoundException {
        String prefix = "";
        if (Paths.get(".").toAbsolutePath().endsWith("tests/.")) {
            prefix = "";
        } else {
            prefix = "tests/";
        }
        return new FileReader(prefix + path);
    }
}
