package datomicJava;

import datomicClient.anomaly.CognitectAnomaly;
import datomicJava.client.api.Datom;
import datomicJava.client.api.async.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static datomic.Util.list;


@RunWith(Parameterized.class)
public class SetupAsync extends SchemaAndData {

    public String system;
    public AsyncClient client;
    public AsyncConnection conn;
    public AsyncTxReport filmDataTx;


    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {"dev-local"},
            // todo: when map-bug is fixed, these should pass:
//            {"peer-server"}
        });
    }

    @Before
    public void setUp() throws ExecutionException, InterruptedException {
        if (system == "dev-local") {
            client = AsyncDatomic.clientDevLocal("Hello system name");
            resetDevLocalDb();

        } else if (system == "peer-server") {
            client = AsyncDatomic.clientPeerServer("k", "s", "localhost:8998");
            conn = ((Right<?, AsyncConnection>) client.connect("hello").get()).right_value();
            resetPeerServerDb();

        } else {
            // todo: live cloud initialization
        }
    }

    public Long txBefore = 0L;
    public Date txInstBefore = null;

    public void resetDevLocalDb() throws ExecutionException, InterruptedException {
        // Re-create db
        client.deleteDatabase("hello").get();
        client.deleteDatabase("world").get();
        client.createDatabase("hello").get();
        conn = ((Right<?, AsyncConnection>) client.connect("hello").get()).right_value();

        // Schema
        AsyncTxReport schemaTx =
            ((Right<?, AsyncTxReport>) conn.transact(schemaDevLocal).get()).right_value();
        txBefore = schemaTx.tx();
        txInstBefore = schemaTx.txInst();

        // Data
        filmDataTx = ((Right<?, AsyncTxReport>) conn.transact(filmData).get()).right_value();
    }

    public void resetPeerServerDb() throws ExecutionException, InterruptedException {
        // Install schema if necessary
        if (
            AsyncDatomic.q(
                "[:find ?e :where [?e :db/ident :movie/title]]",
                conn.db()
            ).get().toString() == "[]"
        ) {
            println("Installing Peer Server hello db schema...");
            conn.transact(schemaPeerServer).get();
        }

        // Retract current data with current ids
        Either<CognitectAnomaly, Stream<?>> firstChunk = AsyncDatomic.q(
            "[:find ?e :where [?e :movie/title _]]",
            conn.db()
        ).get().chunk();
        final ArrayList<AsyncTxReport> lastTxList = new ArrayList<>();
        Stream<?> rows = ((Right<?, Stream<?>>) firstChunk).right_value();
        rows.forEach(row -> {
                try {
                    lastTxList.add(
                        ((Right<?, AsyncTxReport>) conn.transact(
                            list(list(":db/retractEntity", ((List<?>) row).get(0)))
                        ).get()).right_value()
                    );
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        );
        int retractionCount = lastTxList.size();
        if (retractionCount > 0) {
            AsyncTxReport lastTx = lastTxList.get(retractionCount - 1);
            txBefore = lastTx.tx();
            txInstBefore = lastTx.txInst();
        }

        filmDataTx = ((Right<?, AsyncTxReport>) conn.transact(filmData).get()).right_value();
    }


    public boolean isDevLocal() {return system.equals("dev-local");}

    public AsyncDb dbAfter() {return filmDataTx.dbAfter();}

    public long tBefore() {return filmDataTx.basisT();}

    public long tAfter() {return filmDataTx.t();}

    public long txAfter() {return filmDataTx.tx();}

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

    public Date txInstAfter() {return filmDataTx.txInst();}

    // Entity ids of the three films
    public long e1() {return txData().get(1).e();}

    public long e2() {return txData().get(4).e();}

    public long e3() {return txData().get(7).e();}

    public int a1() {return (isDevLocal()) ? 73 : 72;}
    public int a2() {return (isDevLocal()) ? 74 : 73;}
    public int a3() {return (isDevLocal()) ? 75 : 74;}

    // Convenience retriever
    public List<String> films(AsyncDb db) throws ExecutionException, InterruptedException {
        List<String> titles = new ArrayList<>();
        Channel<Stream<?>> chunks = AsyncDatomic.q(filmQuery, db).get();
        Either<CognitectAnomaly, Stream<?>> firstChunk = chunks.chunk();
        Stream<?> rows = ((Right<?, Stream<?>>) firstChunk).right_value();
        if (rows == null)
            return list();
        for (Iterator<List<String>> it = (Iterator<List<String>>) rows.iterator(); it.hasNext(); )
            titles.add(it.next().get(0));
        Collections.sort(titles);
        return titles;
    }
}
