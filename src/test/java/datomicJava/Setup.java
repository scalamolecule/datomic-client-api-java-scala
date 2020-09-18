package datomicJava;

import datomic.Peer;
import datomicJava.client.api.sync.TxReport;
import datomicJava.client.api.Datom;
import datomicJava.client.api.sync.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.Stream;

import static datomic.Util.list;


@RunWith(Parameterized.class)
public class Setup extends SchemaAndData {

    public String system;
    public Client client;
    public Connection conn;
    public TxReport txReport;


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
            resetDevLocalDb();

        } else if (system == "peer-server") {
            client = Datomic.clientPeerServer("myaccesskey", "mysecret", "localhost:8998");
            conn = client.connect("hello");
            resetPeerServerDb();

        } else {
            // todo: live cloud initialization
        }
    }

    public void resetDevLocalDb() {
        // Re-create db
        client.deleteDatabase("hello");
        client.deleteDatabase("world");
        client.createDatabase("hello");
        conn = client.connect("hello");
        conn.transact(schemaDevLocal);
        txReport = conn.transact(data);
    }

    public void resetPeerServerDb() {
        // Install schema if necessary
        if (
            Datomic.q(
                "[:find ?e :where [?e :db/ident :movie/title]]",
                conn.db()
            ).toString() == "[]"
        ) {
            println("Installing Peer Server hello db schema...");
            conn.transact(schemaPeerServer);
        }

        // Retract current data
        Datomic.q("[:find ?e :where [?e :movie/title _]]", conn.db())
            .forEach(row ->
                conn.transact(list(list(":db/retractEntity", row.get(0))))
            );

        txReport = conn.transact(data);
    }


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
    public long e1() {
        return txData().get(1).e();
    }
    public long e2() {
        return txData().get(4).e();
    }
    public long e3() {
        return txData().get(7).e();
    }

    // Ids of the three attributes
    public int a1() {
        if (isDevLocal()) {
            return 73;
        } else {
            return 72;
        }
    }

    // Convenience retriever
    public List<String> films(Db db) {
        List<String> titles = new ArrayList<String>();
        Datomic.q(filmQuery, db).forEach(row ->
            titles.add((String) row.get(0))
        );
        Collections.sort(titles);
        return titles;
    }
}
