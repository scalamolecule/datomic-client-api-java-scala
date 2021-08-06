package datomicJava;

import datomicJava.client.api.Datom;
import datomicJava.client.api.sync.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.Stream;

import static datomic.Util.list;


@RunWith(Parameterized.class)
public abstract class Setup extends SchemaAndData {

    public String system;
    public Client client;
    public Connection conn;
    public TxReport filmDataTx;


    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {"dev-local"},
            {"peer-server"}
        });
    }

    @Before
    public void setUp() throws InterruptedException {
        if (system == "dev-local") {
            client = Datomic.clientDevLocal("test-datomic-client-api-java");
            resetDevLocalDb();

        } else if (system == "peer-server") {
            client = Datomic.clientPeerServer("k", "s", "localhost:8998");
            conn = client.connect("hello");
            resetPeerServerDb();

        } else {
            // todo: live cloud initialization
        }
    }

    public Long txBefore = 0L;
    public Date txInstBefore = null;

    public void resetDevLocalDb() {
        // Re-create db
        client.deleteDatabase("hello");
        client.deleteDatabase("world");
        client.createDatabase("hello");
        conn = client.connect("hello");

        // Schema
        TxReport schemaTx = conn.transact(schemaDevLocal);
        txBefore = schemaTx.tx();
        txInstBefore = schemaTx.txInst();

        // Data
        filmDataTx = conn.transact(filmData);
    }

    public void resetPeerServerDb() throws InterruptedException {
        // Install schema if necessary
        if (Datomic.q("[:find ?e :where [?e :db/ident :movie/title]]", conn.db()).isEmpty()) {
            println("Installing Peer Server hello db schema...");
            conn.transact(schemaPeerServer);
        }

        // Retract current data
        final ArrayList<TxReport> lastTxList = new ArrayList<>();
        Datomic.q("[:find ?e :where [?e :movie/title _]]", conn.db())
            .forEach(row ->
                lastTxList.add(conn.transact(list(list(":db/retractEntity", row.get(0)))))
            );
        int retractionCount = lastTxList.size();
        if (retractionCount > 0) {
            TxReport lastTx = lastTxList.get(retractionCount - 1);
            txBefore = lastTx.tx();
            txInstBefore = lastTx.txInst();
        }

        // Data
        filmDataTx = conn.transact(filmData);
    }


    public boolean isDevLocal() {
        return system.equals("dev-local");
    }

    public Db dbAfter() {
        return filmDataTx.dbAfter();
    }

    public long tBefore() {
        return filmDataTx.basisT();
    }

    public long tAfter() {
        return filmDataTx.t();
    }

    public long txAfter() {
        return filmDataTx.tx();
    }

    private ArrayList<Datom> txDataArray = null;

    public ArrayList<Datom> txData() {
        if (txDataArray == null) {
            Stream<Datom> stream = filmDataTx.txData();
            txDataArray = new ArrayList<Datom>();
            for (java.util.Iterator<Datom> it = stream.iterator(); it.hasNext(); ) {
                txDataArray.add(it.next());
            }
        }
        return txDataArray;
    }

    public Date txInstAfter() {
        return filmDataTx.txInst();
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

    public int a1() {
        return (isDevLocal()) ? 73 : 72;
    }

    public int a2() {
        return (isDevLocal()) ? 74 : 73;
    }

    public int a3() {
        return (isDevLocal()) ? 75 : 74;
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
