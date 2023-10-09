/*
 * BulkLoadIntegrationTest.java
 *
 * Copyright (c) 2023 Snowflake Computing
 */
package com.apple.foundationdb;

import java.util.Random;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests around Bulk Loading. This requires a running FDB instance to work properly;
 * all tests will be skipped if it can't connect to a running instance relatively quickly.
 */
@ExtendWith(RequiresDatabase.class)
class BulkLoadIntegrationTest {
    private static final FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

    @BeforeEach
    @AfterEach
    void clearDatabase() throws Exception {
        /*
         * Empty the database before and after each run, just in case
         */
        try (Database db = fdb.open()) {
            db.run(tr -> {
                tr.clear(new byte[0], new byte[] { (byte) 0xff });
                return null;
            });
        }
    }

    void checkListAndStatus(Database db, Range bulkLoadRange, byte[] bulkLoadId, int expectedStateCode) {
        KeyArrayResult ids = db.listBulkLoads(bulkLoadRange.begin, bulkLoadRange.end).join();
        Assertions.assertEquals(1, ids.getKeys().size());
        System.out.println("ID Expected: " + Arrays.toString(bulkLoadId));
        System.out.println("ID Found: " + Arrays.toString(ids.getKeys().get(0)));
        Assertions.assertArrayEquals(bulkLoadId, ids.getKeys().get(0));

        BulkLoadStatus status = db.getBulkLoadStatus(bulkLoadId).join();
        System.out.println("Status Expected: " + expectedStateCode);
        System.out.println("Status Found: " + status.stateCode);
        Assertions.assertEquals(expectedStateCode, status.stateCode);
    }

    @Test
    void bulkLoadFunctionsTest() throws Exception {
        /*
         * A test that runs through a lifecycle of bulk loading.
         */
        Random rand = new Random();
        byte[] key = new byte[16];
        byte[] value = new byte[8];

        rand.nextBytes(key);
        key[0] = (byte)0x30;
        rand.nextBytes(value);

        System.out.println("Bulk Load database management tests starting");

        Range bulkLoadRange = Range.startsWith(key);
        try (Database db = fdb.open()) {

            System.out.println("Bulk Load: list before start");
            KeyArrayResult idsEmptyBefore = db.listBulkLoads(bulkLoadRange.begin, bulkLoadRange.end).join();
            Assertions.assertEquals(0, idsEmptyBefore.getKeys().size());

            System.out.println("Bulk Load: start");

            byte[] bulkLoadId = db.startBulkLoad(bulkLoadRange.begin, bulkLoadRange.end).join();

            System.out.println("Bulk Load: started: " + Arrays.toString(bulkLoadId));

            checkListAndStatus(db, bulkLoadRange, bulkLoadId, 0);

            System.out.println("Bulk Load: aborting");

            db.abortBulkLoad(bulkLoadId).join();

            System.out.println("Bulk Load: aborted");

            checkListAndStatus(db, bulkLoadRange, bulkLoadId, 3);

            System.out.println("Bulk Load: cleaning");

            db.cleanupBulkLoad(bulkLoadId).join();

            System.out.println("Bulk Load: cleaned");

            KeyArrayResult idsEmptyAfter = db.listBulkLoads(bulkLoadRange.begin, bulkLoadRange.end).join();
            Assertions.assertEquals(0, idsEmptyAfter.getKeys().size());

            System.out.println("Bulk Load database management tests complete!");
        }
    }

    void checkListAndStatus(Tenant tenant, Range bulkLoadRange, byte[] bulkLoadId, int expectedStateCode) {
        KeyArrayResult ids = tenant.listBulkLoads(bulkLoadRange.begin, bulkLoadRange.end).join();
        Assertions.assertEquals(1, ids.getKeys().size());
        System.out.println("ID Expected: " + Arrays.toString(bulkLoadId));
        System.out.println("ID Found: " + Arrays.toString(ids.getKeys().get(0)));
        Assertions.assertArrayEquals(bulkLoadId, ids.getKeys().get(0));

        BulkLoadStatus status = tenant.getBulkLoadStatus(bulkLoadId).join();
        System.out.println("Status Expected: " + expectedStateCode);
        System.out.println("Status Found: " + status.stateCode);
        Assertions.assertEquals(expectedStateCode, status.stateCode);
    }

    @Test
    void bulkLoadFunctionsTenantTest() throws Exception {
        /*
         * A test that runs through a lifecycle of bulk loading.
         */
        Random rand = new Random();
        byte[] key = new byte[16];
        byte[] value = new byte[8];

        rand.nextBytes(key);
        key[0] = (byte)0x30;
        rand.nextBytes(value);

        System.out.println("Bulk Load tenant management tests starting");

        Range bulkLoadRange = Range.startsWith(key);
        byte[] tenantName = "BulkLoadTenant".getBytes();
        try (Database db = fdb.open()) {
            TenantManagement.createTenant(db, tenantName).join();

            System.out.println("Created tenant for test");

            try (Tenant tenant = db.openTenant(tenantName)) {
                System.out.println("Bulk Load: list before start");
                KeyArrayResult idsEmptyBefore = tenant.listBulkLoads(bulkLoadRange.begin, bulkLoadRange.end).join();
                Assertions.assertEquals(0, idsEmptyBefore.getKeys().size());

                System.out.println("Bulk Load: start");

                byte[] bulkLoadId = tenant.startBulkLoad(bulkLoadRange.begin, bulkLoadRange.end).join();

                System.out.println("Bulk Load: started: " + Arrays.toString(bulkLoadId));

                checkListAndStatus(tenant, bulkLoadRange, bulkLoadId, 0);

                System.out.println("Bulk Load: aborting");

                tenant.abortBulkLoad(bulkLoadId).join();

                System.out.println("Bulk Load: aborted");

                checkListAndStatus(tenant, bulkLoadRange, bulkLoadId, 3);

                System.out.println("Bulk Load: cleaning");

                tenant.cleanupBulkLoad(bulkLoadId).join();

                System.out.println("Bulk Load: cleaned");

                KeyArrayResult idsEmptyAfter = tenant.listBulkLoads(bulkLoadRange.begin, bulkLoadRange.end).join();
                Assertions.assertEquals(0, idsEmptyAfter.getKeys().size());

                System.out.println("Bulk Load database management tests complete!");
            }
        }
    }
}
