import clojure.java.api.Clojure;
import clojure.lang.ILookup;

import java.util.Map;

import static datomic.Util.*;

public class AsOfTest {

    public static void main(String[] args) {
        devLocalAsOf();
        System.out.println("========================");
        peerServerAsOf();
    }

    public static void devLocalAsOf() {

        // Setup dev-local client ----------------------------------------

        Clojure.var("clojure.core", "require").invoke(read("datomic.client.api"));

        Object client = Clojure.var("datomic.client.api", "client").invoke(
            read("{:server-type :dev-local, :system \"test\"}")
        );

        Clojure.var("datomic.client.api", "create-database").invoke(
            client, read("{:db-name \"hello\"}")
        );

        Object conn = Clojure.var("datomic.client.api", "connect").invoke(
            client, read("{:db-name \"hello\"}")
        );

        Clojure.var("datomic.client.api", "transact").invoke(
            conn,
            read(
                "{:tx-data [{" +
                    ":db/ident :ns/int " +
                    ":db/valueType :db.type/long " +
                    ":db/cardinality :db.cardinality/one}]}"
            )
        );


        // Original data ----------------------------------------

        Clojure.var("datomic.client.api", "transact").invoke(
            conn, read("{:tx-data [{:ns/int 1}]}")
        );
        Object db1 = Clojure.var("datomic.client.api", "db").invoke(conn);
        Object t1 = ((ILookup) db1).valAt(read(":t"));
        System.out.println(Clojure.var("datomic.client.api", "q").invoke(
            map(
                read(":query"), read("[:find ?i :where [_ :ns/int ?i]]"),
                read(":args"), list(db1)
            )
        )); // [[1]]


        // with ----------------------------------------

        Map dataTx2 = (Map) Clojure.var("datomic.client.api", "with").invoke(
            Clojure.var("datomic.client.api", "with-db").invoke(conn),
            read("{:tx-data [{:ns/int 2}]}")
        );
        Object db2 = dataTx2.get(read(":db-after"));
        System.out.println(Clojure.var("datomic.client.api", "q").invoke(
            map(
                read(":query"), read("[:find ?i :where [_ :ns/int ?i]]"),
                read(":args"), list(db2)
            )
        )); // [[1] [2]]


        // as-of ----------------------------------------

        Object dbAsOf = Clojure.var("datomic.client.api", "as-of").invoke(db2, t1);
        System.out.println(Clojure.var("datomic.client.api", "q").invoke(
            map(
                read(":query"), read("[:find ?i :where [_ :ns/int ?i]]"),
                read(":args"), list(dbAsOf)
            )
        ));

        // Getting: [[1]]  (as expected)
    }


    public static void peerServerAsOf() {

        // Setup peer-server client ----------------------------------------

        // Started peer-server with
        // bin/run -m datomic.peer-server -h localhost -p 8998 -a myaccesskey,mysecret -d hello,datomic:mem://hello

        Clojure.var("clojure.core", "require").invoke(read("datomic.client.api"));

        Object client = Clojure.var("datomic.client.api", "client").invoke(
            read(
                "{" +
                    ":server-type :peer-server, " +
                    ":access-key \"myaccesskey\", " +
                    ":secret \"mysecret\", " +
                    ":endpoint \"localhost:8998\", " +
                    ":validate-hostnames false" +
                    "}"
            )
        );

        Object conn = Clojure.var("datomic.client.api", "connect").invoke(
            client, read("{:db-name \"hello\"}")
        );

        Clojure.var("datomic.client.api", "transact").invoke(
            conn,
            read(
                "{:tx-data [{" +
                    ":db/ident :ns/int " +
                    ":db/valueType :db.type/long " +
                    ":db/cardinality :db.cardinality/one}]}"
            )
        );


        // Original data ----------------------------------------

        Clojure.var("datomic.client.api", "transact").invoke(
            conn, read("{:tx-data [{:ns/int 1}]}")
        );
        Object db1 = Clojure.var("datomic.client.api", "db").invoke(conn);
        Object t1 = ((ILookup) db1).valAt(read(":t"));
        System.out.println(Clojure.var("datomic.client.api", "q").invoke(
            map(
                read(":query"), read("[:find ?i :where [_ :ns/int ?i]]"),
                read(":args"), list(db1)
            )
        )); // [[1]]


        // with ----------------------------------------

        Map dataTx2 = (Map) Clojure.var("datomic.client.api", "with").invoke(
            Clojure.var("datomic.client.api", "with-db").invoke(conn),
            read("{:tx-data [{:ns/int 2}]}")
        );
        Object db2 = dataTx2.get(read(":db-after"));
        System.out.println(Clojure.var("datomic.client.api", "q").invoke(
            map(
                read(":query"), read("[:find ?i :where [_ :ns/int ?i]]"),
                read(":args"), list(db2)
            )
        )); // [[1] [2]]


        // as-of ----------------------------------------

        Object dbAsOf = Clojure.var("datomic.client.api", "as-of").invoke(db2, t1);
        System.out.println(Clojure.var("datomic.client.api", "q").invoke(
            map(
                read(":query"), read("[:find ?i :where [_ :ns/int ?i]]"),
                read(":args"), list(dbAsOf)
            )
        ));

        // Getting  : [[1] [2]]  <<--- Unexpected - seems to still be using db2?
        // Expecting: [[1]]
    }
}
