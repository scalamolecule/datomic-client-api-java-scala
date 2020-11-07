package datomicJava.client.api.async;

import clojure.lang.ExceptionInfo;
import datomicJava.SetupAsync;
import datomicJava.client.api.Datom;
import datomicJava.client.api.DbStats;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static datomic.Util.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThrows;

@FixMethodOrder(MethodSorters.JVM)
public class AsyncDbTest extends SetupAsync {

    public AsyncDbTest(String name) {
        system = name;
    }

    @Test
    public void stats() throws ExecutionException, InterruptedException {
        AsyncDb db = conn.db();
        assertThat(db.dbName(), is("hello"));
        assertThat(db.basisT(), is(tAfter()));
        assertThat(db.asOfT(), is(0L));
        assertThat(db.sinceT(), is(0L));
        assertThat(db.isHistory(), is(false));

        if (isDevLocal()) {
            DbStats dbStats = new DbStats(
                243,
                map(
                    ":db.install/partition", 3L,
                    ":db.install/valueType", 15L,
                    ":db.install/attribute", 30L,
                    ":db/ident", 58L,
                    ":db/valueType", 30L,
                    ":db/tupleType", 2L,
                    ":db/cardinality", 30L,
                    ":db/unique", 1L,
                    ":db/doc", 42L,
                    ":db/txInstant", 7L,
                    ":db/fulltext", 1L,
                    ":fressian/tag", 15L,
                    ":movie/genre", 3L,
                    ":movie/release-year", 3L,
                    ":movie/title", 3L
                )
            );

            assertThat(
                ((Right<?, DbStats>) db.dbStats().get())
                    .right_value().datoms(),
                is(dbStats.datoms())
            );
            assertThat(
                ((Right<?, DbStats>) db.dbStats().get())
                    .right_value().attrs().get(":db.install/partition"),
                is(dbStats.attrs().get(":db.install/partition"))
            );

        } else {
            // Peer server db is not re-created on each test,
            // so we can only test some stable values
            assertThat(
                ((Right<?, DbStats>) db.dbStats().get())
                    .right_value().datoms(),
                is(greaterThan(0L))
            );
            assertThat(
                ((Right<?, DbStats>) db.dbStats().get())
                    .right_value().attrs().get(":db.install/partition"),
                is(3L)
            );
        }

        assertThat(db.asOf(tBefore()).dbName(), is("hello"));
        assertThat(db.asOf(tBefore()).basisT(), is(tAfter()));
        assertThat(db.asOf(tBefore()).asOfT(), is(tBefore()));
        assertThat(db.asOf(tBefore()).sinceT(), is(0L));
        assertThat(db.asOf(tBefore()).isHistory(), is(false));

        assertThat(db.asOf(tAfter()).dbName(), is("hello"));
        assertThat(db.asOf(tAfter()).basisT(), is(tAfter()));
        assertThat(db.asOf(tAfter()).asOfT(), is(tAfter()));
        assertThat(db.asOf(tAfter()).sinceT(), is(0L));
        assertThat(db.asOf(tAfter()).isHistory(), is(false));

        assertThat(db.since(tBefore()).dbName(), is("hello"));
        assertThat(db.since(tBefore()).basisT(), is(tAfter()));
        assertThat(db.since(tBefore()).asOfT(), is(0L));
        assertThat(db.since(tBefore()).sinceT(), is(tBefore()));
        assertThat(db.since(tBefore()).isHistory(), is(false));

        assertThat(db.since(tAfter()).dbName(), is("hello"));
        assertThat(db.since(tAfter()).basisT(), is(tAfter()));
        assertThat(db.since(tAfter()).asOfT(), is(0L));
        assertThat(db.since(tAfter()).sinceT(), is(tAfter()));
        assertThat(db.since(tAfter()).isHistory(), is(false));

        assertThat(db.history().dbName(), is("hello"));
        assertThat(db.history().basisT(), is(tAfter()));
        assertThat(db.history().asOfT(), is(0L));
        assertThat(db.history().sinceT(), is(0L));
        assertThat(db.history().isHistory(), is(true));
    }


    @Test
    public void asOf() throws ExecutionException, InterruptedException {

        // Current state
        assertThat(films(conn.db()), is(threeFilms));

        // State before last tx
        assertThat(films(conn.db().asOf(tBefore())), is(empty()));

        // State after last tx same as current state
        assertThat(films(conn.db().asOf(tAfter())), is(threeFilms));

        // We can use the transaction id too
        assertThat(films(conn.db().asOf(txIdBefore())), is(empty()));
    }


    @Test
    public void since() throws ExecutionException, InterruptedException {
        // State created since previous t
        assertThat(films(conn.db().since(tBefore())), is(threeFilms));

        // Nothing created after
        assertThat(films(conn.db().since(tAfter())), is(empty()));
    }


    @Test
    public void with() throws ExecutionException, InterruptedException {
        Object wDb = ((Right<?, Object>) conn.withDb().get()).right_value();
        AsyncDb db = conn.db();

        // Updated `with` db value
        AsyncDb wDb2 = ((Right<?, AsyncDb>) db.with(wDb, film4).get()).right_value();

        assertThat(films(wDb2), is(fourFilms));

        // Add more data to `wDb2`
        AsyncDb wDb3 = ((Right<?, AsyncDb>) db.with(wDb2.datomicDb(), film5).get()).right_value();
        assertThat(films(wDb3), is(fiveFilms));

        // Current state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }


    @Test
    public void withSingleInvocation() throws ExecutionException, InterruptedException {
        // As a convenience, a single-invocation shorter version of `with`:
        assertThat(
            films(((Right<?, AsyncDb>) conn.widh(film4).get()).right_value()),
            is(fourFilms)
        );

        // Applying another data set still augments the original db
        assertThat(
            films(((Right<?, AsyncDb>) conn.widh(film4and5).get()).right_value()),
            is(fiveFilms)
        );

        // Current state is unaffected
        assertThat(films(conn.db()), is(threeFilms));
    }

    @Test
    public void history() throws ExecutionException, InterruptedException {

        // Not testing Peer Server history since history is accumulating when we
        // can't re-create database for each test without shutting down Peer Server.

        if (isDevLocal()) {

            // Current and history db are currently the same
            assertThat(films(conn.db()), is(threeFilms));
            assertThat(films(conn.db().history()), is(threeFilms));

            // As long as we only add data, current/history will be the same
            AsyncTxReport tx = ((Right<?, AsyncTxReport>) conn.transact(film4).get()).right_value();
            assertThat(films(conn.db()), is(fourFilms));
            assertThat(films(conn.db().history()), is(fourFilms));

            // Now retract the last entity
            Iterator<Datom> it = tx.txData().iterator();
            long lastEid = 0L;
            while (it.hasNext()) lastEid = it.next().e();
            conn.transact(list(list(read(":db/retractEntity"), lastEid))).get(); // await Future

            assertThat(films(conn.db()), is(threeFilms));
            assertThat(films(conn.db().history()), is(fourFilms));

            // History of movie title assertions and retractions
            Stream<?> stream = ((Right<?, Stream<?>>) AsyncDatomic.q(
                "[:find ?tx ?added ?movie-title :where [_ :movie/title ?movie-title ?tx ?added]]",
                conn.db().history() // Use history database
            ).get().chunk()).right_value();
            Iterator<List<Object>> history = (Iterator<List<Object>>) stream.iterator();
            List<String> historyStr = new ArrayList<>();
            while (history.hasNext()) {
                historyStr.add(history.next().toString());
            }
            Collections.sort(historyStr);
            assertThat(
                historyStr,
                is(list(
                    // First tx
                    "[13194139533319 true \"Commando\"]",
                    "[13194139533319 true \"Repo Man\"]",
                    "[13194139533319 true \"The Goonies\"]",

                    // Film 4 added
                    "[13194139533320 true \"Film 4\"]",

                    // Film 4 retracted
                    "[13194139533321 false \"Film 4\"]"
                ))
            );
        }
    }


    @Test
    public void datoms() throws ExecutionException, InterruptedException {
        Iterator<Datom> it = ((Right<?, Stream<Datom>>) conn.db()
            .datoms(":avet", list(read(":movie/title"))).get())
            .right_value().iterator();
        List<String> films = new ArrayList<>();
        while (it.hasNext()) {
            films.add(it.next().v().toString());
        }
        Collections.sort(films);
        assertThat(films, is(threeFilms));
    }

    @Test
    public void indexRange() throws ExecutionException, InterruptedException {

        Iterator<Datom> datoms = ((Right<?, Stream<Datom>>) conn.db()
            .indexRange(":movie/title").get())
            .right_value().iterator();

        List<Datom> datomsCheck = new ArrayList<>();
        datomsCheck.add(new Datom(e2(), a1(), "Commando", txIdAfter(), true));
        datomsCheck.add(new Datom(e3(), a1(), "Repo Man", txIdAfter(), true));
        datomsCheck.add(new Datom(e1(), a1(), "The Goonies", txIdAfter(), true));
        Iterator<Datom> datomsCheckIt = datomsCheck.iterator();

        while (datoms.hasNext()) {
            assertThat(datoms.next(), is(datomsCheckIt.next()));
        }
    }

    @Test
    public void pull() throws ExecutionException, InterruptedException {
        // Pull last movie
        Map entity = ((Right<?, Map<?, ?>>) conn.db().pull("[*]", eid()).get()).right_value();
        assertThat(entity.get(read(":db/id")), is(eid()));
        assertThat(entity.get(read(":movie/title")), is("Repo Man"));
        assertThat(entity.get(read(":movie/genre")), is("punk dystopia"));
        assertThat(entity.get(read(":movie/release-year")), is(1984L));

        // dev-local in-memory db will pull within 1 ms
        if (!isDevLocal()) {
            ExceptionInfo timedOut = assertThrows(
                ExceptionInfo.class,
                () -> conn.db().pull("[*]", eid(), 1, 0, 0)
            );
            assertThat(timedOut.getMessage(), is("Datomic Client Timeout"));
            assertThat(timedOut.getData(), is(
                map(
                    read(":cognitect.anomalies/category"), read(":cognitect.anomalies/interrupted"),
                    read(":cognitect.anomalies/message"), "Datomic Client Timeout"
                )
            ));
        }
    }

    // since 1.0.61.65
    @Test
    public void indexPull() throws ExecutionException, InterruptedException {
        // Pull from :avet index
        Right<?, Stream<?>> right = ((Right<?, Stream<?>>) conn.db().indexPull(
            ":avet",
            "[:movie/title :movie/release-year]",
            "[:movie/release-year 1985]"
        ).get());
        Iterator<Map<?, ?>> entities = (Iterator<Map<?, ?>>) right.right_value().iterator();

        // 2 films pulled from index
        Map<?, ?> film1 = entities.next();
        Map<?, ?> film2 = entities.next();


        assertThat(film1.get(read(":movie/title")), is("The Goonies"));
        assertThat(film1.get(read(":movie/release-year")), is(1985L));

        assertThat(film2.get(read(":movie/title")), is("Commando"));
        assertThat(film2.get(read(":movie/release-year")), is(1985L));
    }
}
