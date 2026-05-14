package com.acme.coordination.ring;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConsistentHashRingTest {

  private final ConsistentHashRing ring = new ConsistentHashRing();

  @Test
  void rangesAgreeWithLookup_manyRandomKeys() {
    List<String> ids = List.of("alpha", "beta", "gamma");
    int vnodes = 32;
    var res = ring.computeRanges(ids, vnodes);
    var assignment = RingAssignment.fromComputeResult(res);
    for (int i = 0; i < 20_000; i++) {
      String key = "k-" + i + "-" + ThreadLocalRandom.current().nextLong();
      long h = Integer.toUnsignedLong(Murmur3_32.hash32(key, 0));
      String byLookup = ring.lookupWorker(ids, vnodes, key);
      String byRange = assignment.workerForRingPoint(h);
      Assertions.assertEquals(
          byLookup,
          byRange,
          () -> "mismatch at h=" + Long.toUnsignedString(h) + " key=" + key);
    }
  }

  @Test
  void ringAssignmentMatchesLookup_forTypedIds() {
    List<String> ids = List.of("w-a", "w-b");
    int vn = 64;
    var res = ring.computeRanges(ids, vn);
    var ra = RingAssignment.fromComputeResult(res);
    var u = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    Assertions.assertEquals(ring.lookupWorker(ids, vn, u.toString()), ra.workerFor(u));
    Assertions.assertEquals(ring.lookupWorker(ids, vn, "42"), ra.workerFor(42));
    Assertions.assertEquals(ring.lookupWorker(ids, vn, Long.toString(1L << 40)), ra.workerFor(1L << 40));
  }

  @Test
  void wrapSegmentNeverProducesStartGreaterThanEnd() {
    var res = ring.computeRanges(List.of("w1", "w2", "w3"), 8);
    for (var e : res.ranges().entrySet()) {
      for (ConsistentHashRing.Range r : e.getValue()) {
        Assertions.assertTrue(
            Long.compareUnsigned(r.start(), r.end()) < 0,
            () -> "invalid range for " + e.getKey() + ": [" + r.start() + ", " + r.end() + ")");
      }
    }
  }

  @Test
  void singleWorkerCoversFullRing() {
    var res = ring.computeRanges(List.of("solo"), 4);
    Assertions.assertEquals(1, res.ranges().size());
    List<ConsistentHashRing.Range> rs = res.ranges().get("solo");
    Assertions.assertEquals(1, rs.size());
    Assertions.assertEquals(0L, rs.get(0).start());
    Assertions.assertEquals(1L << 32, rs.get(0).end());
  }

  @Test
  void lowerBoundMatchesFirstPointAtOrAfterKey() {
    List<ConsistentHashRing.Point> pts =
        List.of(
            new ConsistentHashRing.Point(10L, "a"),
            new ConsistentHashRing.Point(20L, "b"),
            new ConsistentHashRing.Point(30L, "c"));
    Assertions.assertEquals(0, ConsistentHashRing.lowerBound(pts, 5L));
    Assertions.assertEquals(1, ConsistentHashRing.lowerBound(pts, 15L));
    Assertions.assertEquals(3, ConsistentHashRing.lowerBound(pts, 100L));
  }

  @Test
  void ownerScanMatchesLookup_randomMapOrder() {
    List<String> ids = List.of("z", "a", "m");
    int vn = 16;
    var res = ring.computeRanges(ids, vn);
    var shuffled = new java.util.HashMap<>(res.ranges());
    var ra = RingAssignment.of(shuffled);
    for (int i = 0; i < 5000; i++) {
      String key = "key-" + i;
      Assertions.assertEquals(ring.lookupWorker(ids, vn, key), ra.workerForKeyString(key));
    }
  }
}
