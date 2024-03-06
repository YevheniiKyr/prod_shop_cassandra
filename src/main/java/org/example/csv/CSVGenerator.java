package org.example.csv;

import org.example.RandomDateGenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;

public class CSVGenerator {

    private static final String PRODUCT_PAIRS_CSV_FILE = "src/main/java/org/example/csv/product_pairs.csv";
    private static final String PRODUCT_TRIOS_CSV_FILE = "src/main/java/org/example/csv/product_trios.csv";
    private static final String PRODUCT_QUADROS_CSV_FILE = "src/main/java/org/example/csv/product_quadros.csv";
    private static final String UNIVERSAL_TABLE_CSV_FILE = "src/main/java/org/example/csv/universal_table.csv";
    private static final int STORES_AMOUNT = 2;
    private static final int PRODUCTS_AMOUNT = 10;
    private static final int amount = 10000; // Total receipts to generate

    public static void main(String[] args) {
        UUID[] storeIDS = new UUID[STORES_AMOUNT];
        for (int i = 0; i < STORES_AMOUNT; i++) {
            storeIDS[i] = UUID.randomUUID();
        }

        UUID[] productIDS = new UUID[PRODUCTS_AMOUNT];
        for (int i = 0; i < PRODUCTS_AMOUNT; i++) {
            productIDS[i] = UUID.randomUUID();
        }

        try (FileWriter productPairsWriter = new FileWriter(PRODUCT_PAIRS_CSV_FILE);
             FileWriter productTriosWriter = new FileWriter(PRODUCT_TRIOS_CSV_FILE);
             FileWriter productQuadrosWriter = new FileWriter(PRODUCT_QUADROS_CSV_FILE);
             FileWriter universalTableWriter = new FileWriter(UNIVERSAL_TABLE_CSV_FILE)) {

            // Write headers
            universalTableWriter.append("store_id,date,receipt_id,product_id,quantity,price\n");
            productPairsWriter.append("product_id_1,product_id_2\n");
            productTriosWriter.append("product_id_1,product_id_2,product_id_3\n");
            productQuadrosWriter.append("product_id_1,product_id_2,product_id_3,product_id_4\n");

            for (int i = 0; i < amount; i++) {
//                System.out.println(i);
                UUID storeID = storeIDS[(int) (Math.random() * STORES_AMOUNT)];
                UUID receiptID = UUID.randomUUID();
                Date date = RandomDateGenerator.generate(); // Use your RandomDateGenerator here
                int amountOfProductsInReceipt = (int) (Math.random() * 6 + 1);
                UUID[] productIDSInReceipt = new UUID[amountOfProductsInReceipt];
                BigDecimal total = BigDecimal.ZERO;

                for (int j = 0; j < amountOfProductsInReceipt; j++) {
                    BigDecimal price = BigDecimal.valueOf(Math.random() * 6 + 1).setScale(2, RoundingMode.HALF_UP);
                    int quantity = (int) (Math.random() * 6 + 1);
                    UUID productID = productIDS[(int) (Math.random() * PRODUCTS_AMOUNT)];

                    productIDSInReceipt[j] = productID;
                    universalTableWriter.append(String.format("%s,%tc,%s,%s,%d,%s\n", storeID, date, receiptID, productID, quantity, price));
                    total = total.add(price.multiply(BigDecimal.valueOf(quantity)));
                }

                Arrays.sort(productIDSInReceipt, Comparator.comparing(UUID::toString));

                if (productIDSInReceipt.length >= 2) {
                    productPairsWriter.append(String.format("%s,%s\n", productIDSInReceipt[0], productIDSInReceipt[1]));
                }
                if (productIDSInReceipt.length >= 3) {
                    productTriosWriter.append(String.format("%s,%s,%s\n", productIDSInReceipt[0], productIDSInReceipt[1], productIDSInReceipt[2]));
                }
                if (productIDSInReceipt.length >= 4) {
                    productQuadrosWriter.append(String.format("%s,%s,%s,%s\n", productIDSInReceipt[0], productIDSInReceipt[1], productIDSInReceipt[2], productIDSInReceipt[3]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("CSV generation complete.");
    }
}

