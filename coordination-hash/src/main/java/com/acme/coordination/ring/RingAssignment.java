package com.acme.coordination.ring;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Client-side routing using the {@code ranges} map from {@code POST /v1/rings/assign} (same layout as {@link
 * ConsistentHashRing#computeRanges}). Resolves a business id to {@code workerId} via Murmur3-32 (seed 0) on the UTF-8
 * key string from {@link RingIds#toKeyString(Object)}.
 */
public final class RingAssignment {

  private final Map<String, List<ConsistentHashRing.Range>> rangesByWorker;

  private RingAssignment(Map<String, List<ConsistentHashRing.Range>> rangesByWorker) {
    Map<String, List<ConsistentHashRing.Range>> copy = new LinkedHashMap<>();
    for (var e : rangesByWorker.entrySet()) {
      copy.put(e.getKey(), List.copyOf(new ArrayList<>(e.getValue())));
    }
    this.rangesByWorker = Map.copyOf(copy);
  }

  /** Build from the same structure as {@link ConsistentHashRing.ComputeResult#ranges()}. */
  public static RingAssignment fromComputeResult(ConsistentHashRing.ComputeResult result) {
    Objects.requireNonNull(result, "result");
    return new RingAssignment(result.ranges());
  }

  /** Build from a defensive copy of {@code rangesByWorker}. */
  public static RingAssignment of(Map<String, List<ConsistentHashRing.Range>> rangesByWorker) {
    Objects.requireNonNull(rangesByWorker, "rangesByWorker");
    return new RingAssignment(rangesByWorker);
  }

  public Map<String, List<ConsistentHashRing.Range>> rangesByWorker() {
    return rangesByWorker;
  }

  /** Murmur3-32 ring position (unsigned uint32 as Java long). */
  public static long ringPointUtf8(String utf8Key) {
    Objects.requireNonNull(utf8Key, "utf8Key");
    return Integer.toUnsignedLong(Murmur3_32.hash32(utf8Key, 0));
  }

  public long ringPointFor(Object id) {
    return ringPointUtf8(RingIds.toKeyString(id));
  }

  /**
   * @return owning {@code workerId}, or {@code null} if ranges are empty or {@code h} is not covered (should not
   *     happen for a full ring from the server).
   */
  public String workerForRingPoint(long unsigned32AsLong) {
    long h = unsigned32AsLong;
    for (var e : rangesByWorker.entrySet()) {
      for (ConsistentHashRing.Range r : e.getValue()) {
        if (Long.compareUnsigned(r.start(), h) <= 0 && Long.compareUnsigned(h, r.end()) < 0) {
          return e.getKey();
        }
      }
    }
    return null;
  }

  public String workerForKeyString(String utf8Key) {
    return workerForRingPoint(ringPointUtf8(utf8Key));
  }

  public String workerFor(Object id) {
    return workerForKeyString(RingIds.toKeyString(id));
  }
}
