package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Objects;
import java.util.UUID;

public class Queries {
    private static CqlSession session;

    public Queries(CqlSession session) {
        this.session = session;
    }

    static final Instant FROM = LocalDate.of(2019, 1, 1).atStartOfDay(ZoneId.systemDefault()).toInstant();

    static final Instant TO = LocalDate.of(2024, 4, 4).atStartOfDay(ZoneId.systemDefault()).toInstant();

    int amountOfSoldProducts() {
        long startTime = System.nanoTime();

        ResultSet resultSet = session.execute("SELECT SUM(quantity) FROM universal_table");
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("1 Query execution time: " + durationMillis + " ms");
        return Objects.requireNonNull(resultSet.one()).getInt(0);
    }

    ///Порахувати вартість проданого товару
    BigDecimal secondQuery() {
        SimpleStatement statement = SimpleStatement.builder("SELECT SUM(price) FROM universal_table")
                .setTimeout(Duration.ofSeconds(30))
                .build();
        long startTime = System.nanoTime();

        ResultSet resultSet = session.execute(statement);
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("2 query execution time : " + durationMillis + " ms");
        return Objects.requireNonNull(resultSet.one()).getBigDecimal(0);
    }

    //Порахувати вартість проданого товару за період
    BigDecimal thirdQuery() {
        PreparedStatement prepared = session.prepare(
                """
                        SELECT SUM(quantity*price)
                        FROM universal_table
                        WHERE date > ? AND date < ?
                        ALLOW FILTERING
                        """
        );

        BoundStatement bound = prepared.bind(FROM, TO);
        long startTime = System.nanoTime();
        ResultSet resultSet = session.execute(bound);
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("3 Query execution time: " + durationMillis + " ms");
        return Objects.requireNonNull(resultSet.one()).getBigDecimal(0);
    }

    //Порахувати скільки було придбано товару А в мазазині В за період С
    int forthQuery() {
        PreparedStatement prepared = session.prepare(""" 
                SELECT SUM(quantity)
                FROM universal_table
                WHERE store_id = ? AND date > ? AND date < ? AND product_id = ?
                ALLOW FILTERING
                 """);
        BoundStatement bound = prepared.bind(
                UUID.fromString("5f70ef85-74ed-40c5-958d-97f61c6195ec"),
                FROM,
                TO,
                UUID.fromString("4a449305-e6a8-45f9-8a48-3f2129b18690")
        );

        long startTime = System.nanoTime();
        ResultSet resultSet = session.execute(bound);
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("4 Query execution time: " + durationMillis + " ms");
        return Objects.requireNonNull(resultSet.one()).getInt(0);
    }

    //Порахувати скільки було придбано товару А в усіх магазинах за період С
    int fifthQuery() {
        PreparedStatement prepared = session.prepare(""" 
                SELECT SUM(quantity)
                FROM universal_table
                WHERE date > ? AND date < ? AND product_id = ?
                ALLOW FILTERING
                 """);
        BoundStatement bound = prepared.bind(
                FROM,
                TO,
                UUID.fromString("4a449305-e6a8-45f9-8a48-3f2129b18690")
        );

        long startTime = System.nanoTime();

        ResultSet resultSet = session.execute(bound);

        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("5 Query execution time: " + durationMillis + " ms");
        return Objects.requireNonNull(resultSet.one()).getInt(0);
    }

    //Порахувати сумарну виручку магазинів за період С
    int sixthQuery() {
        PreparedStatement prepared = session.prepare(""" 
                SELECT COUNT(*) as amount, product_id_1, product_id_2
                FROM product_pairs
                GROUP BY product_id_1, product_id_2
                 """);
        BoundStatement bound = prepared.bind();
        long startTime = System.nanoTime();

        ResultSet resultSet = session.execute(bound);
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("6 Query execution time: " + durationMillis + " ms");

        return 0;
    }


    int seventhQuery() {
        PreparedStatement prepared = session.prepare(""" 
                SELECT COUNT(*) as amount, product_id_1, product_id_2, product_id_3
                FROM product_trios
                GROUP BY product_id_1, product_id_2, product_id_3
                          
                 """);
        BoundStatement bound = prepared.bind();
        long startTime = System.nanoTime();

        ResultSet resultSet = session.execute(bound);
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("7 Query execution time: " + durationMillis + " ms");
        return 0;
    }

    int eighthQuery() {
        PreparedStatement prepared = session.prepare(""" 
                SELECT COUNT(*) as amount, product_id_1, product_id_2
                FROM product_quadros
                GROUP BY product_id_1, product_id_2 
                 """);
        BoundStatement bound = prepared.bind();
        long startTime = System.nanoTime();

        ResultSet resultSet = session.execute(bound);
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        long durationMillis = duration / 1_000_000;

        System.out.println("8 Query execution time: " + durationMillis + " ms");

        return 0;
    }
}
