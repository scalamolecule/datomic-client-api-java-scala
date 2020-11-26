package datomicJava.client.api.async;

import datomicJava.SetupAsync;
import datomicJava.client.api.Datom;
import javafx.util.Pair;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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


    public static void checkRange(
        AsyncConnection conn,
        Object timePointStart,
        Object timePointEnd,
        List<AsyncTxReport> expected
    ) throws ExecutionException, InterruptedException {
        // Retrieved
        List<Pair<Object, List<Datom>>> retrievedTxs = new ArrayList<>();

        final Iterator<Pair<Object, Iterable<Datom>>> it =
            ((Right<?, Iterable<Pair<Object, Iterable<Datom>>>>)conn
                .txRange(timePointStart, timePointEnd).get())
                .right_value().iterator();

        while (it.hasNext()) {
            Pair<Object, Iterable<Datom>> txs = it.next();
            Iterator<Datom> datoms0 = txs.getValue().iterator();
            List<Datom> datoms1 = new ArrayList<Datom>();
            while(datoms0.hasNext()){
                datoms1.add(datoms0.next());
            }
            retrievedTxs.add(new Pair<Object, List<Datom>>(txs.getKey(), datoms1));
        }

        // Expected
        List<Pair<Object, List<Datom>>> expectedTxs = new ArrayList<>();
        Iterator<AsyncTxReport> it2 = expected.iterator();
        while(it2.hasNext()){
            AsyncTxReport txReport = it2.next();
            Iterator<Datom> datoms0 = txReport.txData().iterator();
            List<Datom> datoms1 = new ArrayList<Datom>();
            while(datoms0.hasNext()) {
                datoms1.add(datoms0.next());
            }
            expectedTxs.add(new Pair<Object, List<Datom>>(txReport.t(), datoms1));
        }
        assertThat(retrievedTxs, is(expectedTxs));
    }

    public static void checkUntil(
        AsyncConnection conn,
        Object timePointEnd,
        Object expectedLastT
    ) throws ExecutionException, InterruptedException {
        final Object[] n = {0L};
        ((Right<?, Iterable<Pair<Object, Iterable<Datom>>>>)conn
            .txRange(0, timePointEnd).get())
            .right_value().forEach(txs -> n[0] = txs.getKey());
        assertThat(n[0], is(expectedLastT));
    }

    @Test
    public void txRange() throws ExecutionException, InterruptedException {

        // Getting all transactions (!) -----------------------------------

        // Lazy retrieval with Iterable
        final Iterator<Pair<Object, Iterable<Datom>>> it =
            ((Right<?, Iterable<Pair<Object, Iterable<Datom>>>>)conn.txRange().get())
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
            ((Right<?, Pair<Object, Datom[]>[]>)conn.txRangeArray().get()).right_value();
        Pair<Object, Datom[]> lastTx2 = it2[it2.length - 1];
        assertThat(lastTx2.getKey(), is(tAfter()));
        assertThat(lastTx2.getValue()[0], is(
            new Datom(txAfter(), 50, txInstAfter(), txAfter(), true)
        ));

        // Get range from timePointStart to timePointEnd ------------------

        AsyncTxReport txReport4 = ((Right<?, AsyncTxReport>) conn.transact(film4).get()).right_value();

        Thread.sleep(5); // Make sure that date's don't share same ms

        AsyncTxReport txReport5 = ((Right<?, AsyncTxReport>) conn.transact(film5).get()).right_value();

        Thread.sleep(5); // Make sure that date's don't share same ms

        AsyncTxReport txReport6 = ((Right<?, AsyncTxReport>) conn.transact(film6).get()).right_value();

        Long tx4 = txReport4.tx();
        Long tx5 = txReport5.tx();
        Long tx6 = txReport6.tx();

        Long t4 = txReport4.t();
        Long t5 = txReport5.t();
        Long t6 = txReport6.t();

        Date txInstant4 = txReport4.txInst();
        Date txInstant6 = txReport6.txInst();

        // timePointStart is after timePointEnd - returns Nil
        checkRange(conn, tx5, tx4, list());

        // timePointEnd is exclusive, so tx4 is not considered
        checkRange(conn, tx4, tx4, list());

        // tx 4
        checkRange(conn, tx4, tx5, list(txReport4));

        // tx 4-5
        checkRange(conn, tx4, tx6, list(txReport4, txReport5));

        // To get until the last tx, set timePointEnd to 0
        checkRange(conn, tx4, 0, list(txReport4, txReport5, txReport6));
        checkRange(conn, tx5, 0, list(txReport5, txReport6));
        checkRange(conn, tx6, 0, list(txReport6));

        // To get from first tx, set timePointStart to 0
        checkUntil(conn, txAfter(), tBefore());
        checkUntil(conn, tx4, tAfter());
        checkUntil(conn, tx5, t4);
        checkUntil(conn, tx6, t5);


        // Getting txRange with t/tx/txInst

        // Using time t
        checkRange(conn, t4, t6, list(txReport4, txReport5));

        // Using transaction id
        checkRange(conn, tx4, tx6, list(txReport4, txReport5));

        // Using Date
        checkRange(conn, txInstant4, txInstant6, list(txReport4, txReport5));
    }
}
