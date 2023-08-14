/*
 * ReadMetricsIntegrationTest.java
 *
 * Copyright (c) 2023 Snowflake Computing
 */

package com.apple.foundationdb;

import com.apple.foundationdb.tuple.ByteArrayUtil;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for working with read metrics
 */
@ExtendWith(RequiresDatabase.class)
class ReadMetricsIntegrationTest {
  private static final FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

  private void checkReadMetrics(ReadMetrics metrics, AtomicInteger counter, long startMillis) {
    if (System.currentTimeMillis() - startMillis > 5000) {
      Assertions.assertTrue(metrics.getServerBusyness() > 0);
    } else {
      Assertions.assertTrue(metrics.getServerBusyness() >= 0);
    }
    Assertions.assertTrue(metrics.getServerBusyness() <= 1);
    Assertions.assertTrue(metrics.getRangeBusyness() >= 0);
    Assertions.assertTrue(metrics.getRangeBusyness() <= 1);

    if (metrics.getRangeBusyness() > 0) {
      counter.incrementAndGet();
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testGetReadMetrics() throws Exception {
  	long start = System.currentTimeMillis();
  	AtomicInteger valueReadsWithBusyness = new AtomicInteger();
    AtomicInteger keyReadsWithBusyness = new AtomicInteger();
    AtomicInteger rangeReadsWithBusyness = new AtomicInteger();
    Random random = new Random();

    byte[] key = "read_metrics/key".getBytes("UTF-8");
    byte[] endKey = "read_metrics/z".getBytes("UTF-8");

    try (Database db = fdb.open()) {
     db.run(tr -> {
       try {
         for(int i = 0; i < 1000; ++i) {
           tr.set(ByteArrayUtil.join(key, String.format("%08d", i).getBytes("UTF-8")), new byte[0]);
         }
         return null;
       } catch (Exception e) {
         throw new RuntimeException(e);
       }
     });

      while(System.currentTimeMillis() - start < 10000) {
        db.run(tr -> {
          int op = random.nextInt(3);
          if (op == 0) {
            checkReadMetrics(tr.getExt(key).join(), valueReadsWithBusyness, start);
          } else if (op == 1) {
            checkReadMetrics(tr.getKeyExt(KeySelector.firstGreaterOrEqual(key)).join(), keyReadsWithBusyness, start);
          } else {
            List<KeyValue> kvs = tr.getRange(key, endKey).asList().join();
            for(KeyValue kv : kvs) {
              checkReadMetrics(kv, rangeReadsWithBusyness, start);
            }
          }

          return null;
        });
      }

      Assertions.assertNotEquals(valueReadsWithBusyness.get(), 0);
      Assertions.assertNotEquals(keyReadsWithBusyness.get(), 0);
      Assertions.assertNotEquals(rangeReadsWithBusyness.get(), 0);
    }
  }
}