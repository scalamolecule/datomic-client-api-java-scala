package datomicJava;

import datomic.Peer;
import datomicJava.client.api.async.Either;
import datomicJava.client.api.async.Right;
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
    public AsyncTxReport txReport;


    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {"dev-local"},
            // todo: run when map-bug is fixed
//            {"peer-server"}
        });
    }

    @Before
    public void setUp() throws ExecutionException, InterruptedException {
        if (system == "dev-local") {
            client = AsyncDatomic.clientDevLocal("Hello system name");
            resetDevLocalDb();

        } else if (system == "peer-server") {
            client = AsyncDatomic.clientPeerServer("myaccesskey", "mysecret", "localhost:8998");
            conn = ((Right<?, AsyncConnection>) client.connect("hello").get()).right_value();
            resetPeerServerDb();

        } else {
            // todo: live cloud initialization
        }
    }

    public void resetDevLocalDb() throws ExecutionException, InterruptedException {
        // Re-create db
        client.deleteDatabase("hello").get();
        client.deleteDatabase("world").get();
        client.createDatabase("hello").get();
        conn = ((Right<?, AsyncConnection>) client.connect("hello").get()).right_value();
        conn.transact(schemaDevLocal).get();
        txReport = ((Right<?, AsyncTxReport>) conn.transact(data).get()).right_value();
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
        Stream<?> rows = ((Right<?, Stream<?>>) firstChunk).right_value();
        rows.forEach(row -> {
                try {
                    conn.transact(
                        list(list(":db/retractEntity", ((List<?>) row).get(0)))
                    ).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        );

        txReport = ((Right<?, AsyncTxReport>) conn.transact(data).get()).right_value();
    }


    public boolean isDevLocal() {
        return system.equals("dev-local");
    }

    // Databases before and after last tx (after == current)
    public AsyncDb dbBefore() {
        return txReport.dbBefore();
    }

    public AsyncDb dbAfter() {
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
            for (Iterator<Datom> it = stream.iterator(); it.hasNext(); ) {
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
