package datomicJava.client.api.sync;

import datomic.Util;
import datomicJava.Setup;
import datomicJava.client.api.Datom;
import javafx.util.Pair;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Iterator;

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
    public void transact() {
        assertThat(films(conn.db()), is(threeFilms));
        conn.transact(list(
            map(
                read(":movie/title"), "Film 4"
            )
        ));
        assertThat(films(conn.db()), is(fourFilms));

        // Applying empty list of stmts returns empty TxReport without touching the db
        assertThat(conn.transact(list()), is(new TxReport(Util.map())));
    }


    @Test
    public void txRange() {

        // Limit -1 sets no-limit
        // (necessary for Peer Server datom accumulation exceeding default 1000)

        // Lazy retrieval with Iterable
        final Iterator<Pair<Object, Iterable<Datom>>> it = conn.txRange(-1).iterator();
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
    }
}
