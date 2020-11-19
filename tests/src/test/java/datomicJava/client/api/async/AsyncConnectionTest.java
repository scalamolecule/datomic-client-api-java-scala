package datomicJava.client.api.async;

import datomicJava.SetupAsync;
import datomicJava.client.api.Datom;
import javafx.util.Pair;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static datomic.Util.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@FixMethodOrder(MethodSorters.JVM)
public class AsyncConnectionTest extends SetupAsync {

    public AsyncConnectionTest(String name) {
        system = name;
    }


    @Test
    public void db() {
        // Test if repeated calls do conn.db returns the same db value (/object)
        AsyncDb db = conn.db();

        if (isDevLocal()) {
            // Dev-local connection returns same database object
            assertThat(conn.db(), is(db));
        } else {
            // Peer Server connection returns new database object
            assertThat(conn.db(), not(db));
        }
    }


    @Test
    public void sync() {
        if (isDevLocal()) {
            // Same db object
            assertThat(conn.sync(tAfter()), is(dbAfter()));
        } else {
            // todo? Does sync call need to create a new db object
            //  or could it be memoized/cached?
            // New db object
            assertThat(conn.sync(tAfter()), not(dbAfter()));
        }
    }


    @Test
    public void transact() throws ExecutionException, InterruptedException {
        assertThat(films(conn.db()), is(threeFilms));
        conn.transact(list(
            map(
                read(":movie/title"), "Film 4"
            )
        )).get(); // Await future completion
        assertThat(films(conn.db()), is(fourFilms));


        // Applying empty list of stmts returns empty TxReport without touching the db
        Either emptyResult = conn.transact(list()).get();
        AsyncTxReport txReport = ((Right<?, AsyncTxReport>) emptyResult).right_value();
        assertThat(txReport.rawTxReport().isEmpty(), is(true));
    }


    @Test
    public void txRange() throws ExecutionException, InterruptedException {

        // Limit -1 sets no-limit
        // (necessary for Peer Server datom accumulation exceeding default 1000)

        // Lazy retrieval with Iterable
        final Iterator<Pair<Object, Iterable<Datom>>> it =
            ((Right<?, Iterable<Pair<Object, Iterable<Datom>>>>)conn.txRange(-1).get())
                .right_value().iterator();
        Pair<Object, Iterable<Datom>> lastTx = it.next();
        while (it.hasNext()) {
            lastTx = it.next();
        }
        assertThat(lastTx.getKey(), is(tAfter()));
        assertThat(lastTx.getValue().iterator().next(), is(
            new Datom(txAfter(), 50, txInstAfter(), txAfter(), true)
        ));

        // Array
        final Pair<Object, Datom[]>[] it2 =
            ((Right<?, Pair<Object, Datom[]>[]>)conn.txRangeArray(-1).get()).right_value();
        Pair<Object, Datom[]> lastTx2 = it2[it2.length - 1];
        assertThat(lastTx2.getKey(), is(tAfter()));
        assertThat(lastTx2.getValue()[0], is(
            new Datom(txAfter(), 50, txInstAfter(), txAfter(), true)
        ));
    }
}
