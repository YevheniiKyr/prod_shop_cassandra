package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import java.nio.file.Paths;


public class Main {
    private static void initSession() {
        session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get("src/main/java/org/example/secure-connect-product-shop.zip"))
                .withAuthCredentials(Config.USERNAME, Config.PASSWORD)
                .withKeyspace("default_keyspace")
                .build();
    }

    public static CqlSession session;

    public static void main(String[] args) {
        try {
            initSession();
//            DBManagementScripts dbManagementScripts = new DBManagementScripts(session);
//            dbManagementScripts.fillDBFromCSV();
            Queries queries = new Queries(session);

            System.out.println(queries.amountOfSoldProducts());
            System.out.println(queries.secondQuery());
            System.out.println(queries.thirdQuery());
            System.out.println(queries.forthQuery());
            System.out.println(queries.fifthQuery());
            System.out.println(queries.sixthQuery());
            System.out.println(queries.seventhQuery());
            System.out.println(queries.eighthQuery());

            closeSession();

        } finally {
            closeSession();
        }

        System.out.println("Query executed successfully.");
        session.close();
    }

    private static void closeSession() {
        session.close();
    }

}
