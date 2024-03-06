package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

public class DBManagementScripts {
    private static CqlSession session;

    public DBManagementScripts(CqlSession session) {
        this.session = session;
    }

    void createDB() {
        session.execute("""
                CREATE TABLE default_keyspace.universal_table (
                        store_id uuid,
                        date timestamp,
                        receipt_id uuid,
                        product_id uuid,
                        price decimal,
                        quantity int,
                PRIMARY KEY (store_id, date, product_id, receipt_id)
                )
                """);

        session.execute("""
                CREATE TABLE default_keyspace.product_pairs (
                    product_id_1 uuid,
                    product_id_2 uuid,
                    PRIMARY KEY (product_id_1, product_id_2)
                )
                """);

        session.execute("""
                CREATE TABLE default_keyspace.product_trios (
                    product_id_1 uuid,
                    product_id_2 uuid,
                    product_id_3 uuid,
                    PRIMARY KEY (product_id_1, product_id_2,product_id_3)
                )
                """);

        session.execute("""
                CREATE TABLE default_keyspace.product_quadros (
                    product_id_1 uuid,
                    product_id_2 uuid,
                    product_id_3 uuid,
                    product_id_4 uuid,
                    PRIMARY KEY (product_id_1, product_id_2, product_id_3, product_id_4)
                )
                """);
    }

    void fillDBFromCSV() {
        fillUniversalTableFromCSV();
        fillProductPairsFromCSV();
        fillProductTriosFromCSV();
        fillProductQuadrosFromCSV();
    }

    private void fillProductQuadrosFromCSV() {
        try (BufferedReader br = new BufferedReader(new FileReader("src\\main\\java\\org\\example\\csv\\product_quadros.csv"))) {
            String line;
            br.readLine(); // Skip the header row
            int cycle = 0;
            while ((line = br.readLine()) != null) {
                if (cycle % 1000 == 0) System.out.println(cycle);
                cycle++;
                String[] columns = line.split(",");
                if (columns.length == 2) {
                    UUID product_id_1 = UUID.fromString(columns[0]);
                    UUID product_id_2 = UUID.fromString(columns[1]);
                    UUID product_id_3 = UUID.fromString(columns[1]);
                    UUID product_id_4 = UUID.fromString(columns[1]);
                    // Insert the product pair into the Cassandra table
                    String cql = "INSERT INTO product_quadros (product_id_1, product_id_2, product_id_3, product_id_4) VALUES (?, ?, ?, ?)";
                    session.execute(SimpleStatement.builder(cql)
                            .addPositionalValue(product_id_1)
                            .addPositionalValue(product_id_2)
                            .addPositionalValue(product_id_3)
                            .addPositionalValue(product_id_4)
                            .build());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fillProductTriosFromCSV() {
        try (BufferedReader br = new BufferedReader(new FileReader("src\\main\\java\\org\\example\\csv\\product_trios.csv"))) {
            String line;
            br.readLine(); // Skip the header row
            int cycle = 0;
            while ((line = br.readLine()) != null) {
                if (cycle % 1000 == 0) System.out.println(cycle);
                cycle++;
                String[] columns = line.split(",");
                if (columns.length == 3) {
                    UUID product_id_1 = UUID.fromString(columns[0]);
                    UUID product_id_2 = UUID.fromString(columns[1]);
                    UUID product_id_3 = UUID.fromString(columns[2]);

                    // Insert the product pair into the Cassandra table
                    String cql = "INSERT INTO product_trios (product_id_1, product_id_2, product_id_3) VALUES (?, ?, ?)";
                    session.execute(SimpleStatement.builder(cql)
                            .addPositionalValue(product_id_1)
                            .addPositionalValue(product_id_2)
                            .addPositionalValue(product_id_3)
                            .build());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fillProductPairsFromCSV() {
        try (BufferedReader br = new BufferedReader(new FileReader("src\\main\\java\\org\\example\\csv\\product_pairs.csv"))) {
            String line;
            br.readLine(); // Skip the header row
            int cycle = 0;
            while ((line = br.readLine()) != null) {
                if (cycle % 1000 == 0) System.out.println(cycle);
                cycle++;
                String[] columns = line.split(",");
                if (columns.length == 2) {
                    UUID product_id_1 = UUID.fromString(columns[0]);
                    UUID product_id_2 = UUID.fromString(columns[1]);

                    // Insert the product pair into the Cassandra table
                    String cql = "INSERT INTO product_pairs (product_id_1, product_id_2) VALUES (?, ?)";
                    session.execute(SimpleStatement.builder(cql)
                            .addPositionalValue(product_id_1)
                            .addPositionalValue(product_id_2)
                            .build());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fillUniversalTableFromCSV() {
        try (BufferedReader br = new BufferedReader(new FileReader("src\\main\\java\\org\\example\\csv\\universal_table.csv"))) {
            String line;
            // Skip the header
            br.readLine();
            int cycle = 0;
            while ((line = br.readLine()) != null) {
                if (cycle % 1000 == 0) System.out.println(cycle);
                cycle++;

                String[] fields = line.split(",");
                if (fields.length == 6) {

                    UUID storeId = UUID.fromString(fields[0]);
                    Instant date = Utils.stringDateToInstant(fields[1]); // Ensure this matches your date format in Cassandra
                    UUID receiptId = UUID.fromString(fields[2]);
                    UUID productId = UUID.fromString(fields[3]);
                    int quantity = Integer.parseInt(fields[4]);
                    double price = Double.parseDouble(fields[5]);

                    // Insert the data into Cassandra
                    String cql = "INSERT INTO universal_table (store_id, date, receipt_id, product_id, quantity, price) VALUES (?, ?, ?, ?, ?, ?)";

                    session.execute(SimpleStatement.newInstance(cql, storeId, date, receiptId, productId, quantity, price));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void fillDB(int amount) {

        final int STORES_AMOUNT = 2;
        final int PRODUCTS_AMOUNT = 10;

        UUID[] storeIDS = new UUID[STORES_AMOUNT];
        storeIDS[0] = UUID.randomUUID();
        storeIDS[1] = UUID.randomUUID();

        UUID[] productIDS = new UUID[PRODUCTS_AMOUNT];
        for (int i = 0; i < 10; i++) {
            productIDS[i] = UUID.randomUUID();
        }

        for (int i = 0; i < amount; i++) {
            System.out.println(i);
            int amountOfProductsInReceipt = (int) (Math.random() * 6 + 1);
            BigDecimal[] productsPrices = new BigDecimal[amountOfProductsInReceipt];
            int[] quantities = new int[amountOfProductsInReceipt];

            for (int j = 0; j < amountOfProductsInReceipt; j++) {
                BigDecimal price = BigDecimal.valueOf((Math.random() * 6 + 1)).setScale(2, RoundingMode.HALF_UP);
                productsPrices[j] = price;
            }

            for (int j = 0; j < amountOfProductsInReceipt; j++) {
                int quantity = (int) (Math.random() * 6 + 1);
                quantities[j] = quantity;
            }

            UUID storeID = storeIDS[(int) (Math.random() * 2)];
            UUID[] productIDSInReceipt = new UUID[amountOfProductsInReceipt];
            for (int j = 0; j < productIDSInReceipt.length; j++) {
                productIDSInReceipt[j] = productIDS[(int) (Math.random() * 10)];
            }

            Date date = RandomDateGenerator.generate();
            Instant dateForCassandra = date.toInstant();
            UUID receiptID = UUID.randomUUID();


            for (int m = 0; m < amountOfProductsInReceipt; m++) {
                session.execute("INSERT INTO universal_table (store_id , date , receipt_id , product_id, quantity, price) VALUES (?, ?, ?, ?, ?, ?)",
                        storeID, dateForCassandra, receiptID, productIDSInReceipt[m], quantities[m], productsPrices[m]);
            }

            //Сортування IDS
            productIDSInReceipt = Arrays.stream(productIDSInReceipt).sorted(Comparator.comparing(UUID::toString)).toArray(UUID[]::new);


            if (productIDSInReceipt.length >= 2) {
                session.execute("INSERT INTO product_pairs (product_id_1 , product_id_2) VALUES (?, ?)",
                        productIDSInReceipt[0], productIDSInReceipt[1]);
            }
            if (productIDSInReceipt.length >= 3) {
                session.execute("INSERT INTO product_trios (product_id_1 , product_id_2, product_id_3) VALUES (?, ?, ?)",
                        productIDSInReceipt[0], productIDSInReceipt[1], productIDSInReceipt[2]);
            }
            if (productIDSInReceipt.length >= 4) {
                session.execute("INSERT INTO product_quadros (product_id_1 , product_id_2, product_id_3, product_id_4) VALUES (?, ?, ?, ?)",
                        productIDSInReceipt[0], productIDSInReceipt[1], productIDSInReceipt[2], productIDSInReceipt[3]);
            }
        }
    }

    void clearDB() {
        session.execute("TRUNCATE universal_table");
        session.execute("TRUNCATE product_pairs");
        session.execute("TRUNCATE product_trios");
        session.execute("TRUNCATE product_quadros");
    }


}
