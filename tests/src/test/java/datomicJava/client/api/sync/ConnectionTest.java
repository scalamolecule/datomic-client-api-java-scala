package datomicJava.client.api.sync;

import datomic.Util;
import datomicJava.Setup;
import datomicJava.client.api.Datom;
import javafx.util.Pair;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static datomic.Util.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@FixMethodOrder(MethodSorters.JVM)
public class ConnectionTest extends Setup {

    public ConnectionTest(String name) {
        system = name;
    }


    @Test
    public void db() {
        // Test if repeated calls do conn.db returns the same db value (/object)
        Db db = conn.db();

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
    public void transactJavaStmts() {
        assertThat(films(conn.db()), is(threeFilms));
        conn.transact(list(
            map(
                read(":movie/title"), "Film 4"
            )
        ));
        assertThat(films(conn.db()), is(fourFilms));

        // Transacting empty list of stmts creates transaction with timestamp only
        TxReport txReport = conn.transact(list());
        Iterator<Datom> txData = txReport.txData().iterator();
        Datom txInstantDatom = txData.next();
        // Only tx instant datom asserted
        assertThat(txData.hasNext(), is(false));
        assertThat(txInstantDatom, is(
            new Datom(txReport.tx(), 50, txReport.txInst(), txReport.tx(), true)
        ));
    }

    @Test
    public void transactEdnFile() throws FileNotFoundException {
        assertThat(films(conn.db()), is(threeFilms));
        conn.transact(getFileReader("resources/film4.edn"));
        assertThat(films(conn.db()), is(fourFilms));
    }

    @Test
    public void transactEdnString() {
        assertThat(films(conn.db()), is(threeFilms));
        conn.transact("[ {:movie/title \"Film 4\"} ]");
        assertThat(films(conn.db()), is(fourFilms));
    }


    public static void checkRange(
        Connection conn,
        Object timePointStart,
        Object timePointEnd,
        List<TxReport> expected
    ) {
        // Retrieved
        List<Pair<Object, List<Datom>>> retrievedTxs = new ArrayList<>();
        Iterator<Pair<Object, Iterable<Datom>>> it =
            conn.txRange(timePointStart, timePointEnd).iterator();
        while (it.hasNext()) {
            Pair<Object, Iterable<Datom>> txs = it.next();
            Iterator<Datom> datoms0 = txs.getValue().iterator();
            List<Datom> datoms1 = new ArrayList<Datom>();
            while (datoms0.hasNext()) {
                datoms1.add(datoms0.next());
            }
            retrievedTxs.add(new Pair<Object, List<Datom>>(txs.getKey(), datoms1));
        }

        // Expected

        List<Pair<Object, List<Datom>>> expectedTxs = new ArrayList<>();
        Iterator<TxReport> it2 = expected.iterator();
        while (it2.hasNext()) {
            TxReport txReport = it2.next();
            Iterator<Datom> datoms0 = txReport.txData().iterator();
            List<Datom> datoms1 = new ArrayList<Datom>();
            while (datoms0.hasNext()) {
                datoms1.add(datoms0.next());
            }
            expectedTxs.add(new Pair<Object, List<Datom>>(txReport.t(), datoms1));
        }
        assertThat(retrievedTxs, is(expectedTxs));
    }

    public static void checkUntil(
        Connection conn,
        Object timePointEnd,
        Object expectedLastT
    ) {
        final Object[] n = {0L};
        conn.txRange(0, timePointEnd).forEach(txs -> n[0] = txs.getKey());
        assertThat(n[0], is(expectedLastT));
    }

    @Test
    public void txRange() throws InterruptedException {

        // Getting all transactions (!) -----------------------------------

        // Lazy retrieval with Iterable
        final Iterator<Pair<Object, Iterable<Datom>>> it = conn.txRange().iterator();
        Pair<Object, Iterable<Datom>> lastTx = it.next();
        while (it.hasNext()) {
            lastTx = it.next();
        }
        assertThat(lastTx.getKey(), is(tAfter()));
        assertThat(lastTx.getValue().iterator().next(), is(
            new Datom(txAfter(), 50, txInstAfter(), txAfter(), true)
        ));

        // Array
        final Pair[] it2 = conn.txRangeArray(-1);
        Pair<Object, Datom[]> lastTx2 = (Pair<Object, Datom[]>) it2[it2.length - 1];
        assertThat(lastTx2.getKey(), is(tAfter()));
        assertThat(lastTx2.getValue()[0], is(
            new Datom(txAfter(), 50, txInstAfter(), txAfter(), true)
        ));

        // Get range from timePointStart to timePointEnd ------------------

        TxReport txReport4 = conn.transact(film4);

        Thread.sleep(5); // Make sure that date's don't share same ms

        TxReport txReport5 = conn.transact(film5);

        Thread.sleep(5); // Make sure that date's don't share same ms

        TxReport txReport6 = conn.transact(film6);

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
