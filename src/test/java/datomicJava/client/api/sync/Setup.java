package datomicJava.client.api.sync;

import clojure.lang.PersistentVector;
import datomicJava.client.api.Datom;
import datomic.Peer;
import datomicJava.SchemaAndData;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.Stream;

import static datomic.Util.list;


@RunWith(Parameterized.class)
public class Setup extends SchemaAndData {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {"dev-local"},
            {"peer-server"}
        });
    }

    @Before
    public void setUp() {
        if (system == "dev-local") {
            client = Datomic.clientDevLocal("Hello system name");

            // Re-create db
            client.deleteDatabase("hello");
            client.deleteDatabase("world");
            client.createDatabase("hello");
            conn = client.connect("hello");
            conn.transact(schemaDevLocal);

            txReport = conn.transact(data);

        } else if (system == "peer-server") {

            client = Datomic.clientPeerServer("myaccesskey", "mysecret", "localhost:8998");

            // Using the db associated with the Peer Server connection
            conn = client.connect("hello");

            // Install schema if necessary
            String x = Datomic.q(
                "[:find ?e :where [?e :db/ident :movie/title]]",
                conn.db()
            ).toString();

            println("XX " + x);

            if (x == "[]") {
                println("Installing Peer Server hello db schema...");
                conn.transact(schemaPeerServer);
            }

            // Retract current data
            Datomic.q("[:find ?e :where [?e :movie/title _]]", conn.db())
                .forEach(row ->
                    conn.transact(list(list(":db/retractEntity", row.get(0))))
                );

            txReport = conn.transact(data);

        } else {
            // todo: live cloud initialization
        }
    }

    String system = "dev-local";

    Client client;
    Connection conn;
    TxReport txReport;

    public boolean isDevLocal() {
        return system.equals("dev-local");
    }

    // Databases before and after last tx (after == current)
    public Db dbBefore() {
        return txReport.dbBefore();
    }

    public Db dbAfter() {
        return txReport.dbAfter();
    }

    // Get t before and after last tx
    public long tBefore() {
        return dbBefore().basisT();
    }

    public long tAfter() {
        return dbAfter().basisT();
    }

    public long txIdBefore() {
        return (long) Peer.toTx(tBefore());
    }

    public long txIdAfter() {
        return (long) Peer.toTx(tAfter());
    }

    private ArrayList<Datom> txDataArray = null;

    public ArrayList<Datom> txData() {
        if (txDataArray == null) {
            Stream<Datom> stream = txReport.txData();
            txDataArray = new ArrayList<Datom>();
            for (java.util.Iterator<Datom> it = stream.iterator(); it.hasNext(); ) {
                txDataArray.add(it.next());
            }
        }
        return txDataArray;
    }

    public Date txInst() {
        return (Date) txData().get(0).v();
    }

    public long eid() {
        return txData().get(txData().size() - 1).e();
    }

    // Entity ids of the three films
    long e1() {
        return txData().get(1).e();
    }

    long e2() {
        return txData().get(2).e();
    }

    long e3() {
        return txData().get(3).e();
    }


    // Ids of the three attributes
    int a1() {
        if (isDevLocal()) {
            return 73;
        } else {
            return 63;
        }
    }

    int a2() {
        if (isDevLocal()) {
            return 74;
        } else {
            return 64;
        }
    }

    int a3() {
        if (isDevLocal()) {
            return 75;
        } else {
            return 65;
        }
    }

    // Convenience retriever
    List<String> films(Db db) {
        List<String> titles = new ArrayList<String>();
        Datomic.q(filmQuery, db).forEach(row ->
            titles.add((String) row.get(0))
        );
        Collections.sort(titles);
        return titles;
    }

    // Convenience sorter
    List<String> sortStrings(Db db, String query) {
        List<String> strings = new ArrayList<String>();
        Datomic.q(query, db).forEach(row ->
            strings.add((String) row.get(0))
        );
        Collections.sort(strings);
        return strings;
    }
}
