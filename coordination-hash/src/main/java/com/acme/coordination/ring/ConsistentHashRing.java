package com.acme.coordination.ring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * UInt32 ring (0 .. 2^32-1) with Murmur3-32 vnode positions. A key maps to the first vnode at or after the key
 * walking clockwise (Ketama-style). {@link #computeRanges} returns half-open uint64 intervals {@code [start, end)}
 * in that space (end may be 2^32); wrap-around ownership is split into two ranges.
 */
public final class ConsistentHashRing {

  private static final long UINT32_SPACE = 1L << 32;
  private static final long UINT32_MAX = UINT32_SPACE - 1L;

  private static final Comparator<Point> POINT_ORDER =
      Comparator.comparingLong(Point::pos).thenComparing(Point::workerId);

  public ComputeResult computeRanges(List<String> workerIds, int virtualNodes) {
    if (workerIds == null || workerIds.isEmpty()) {
      return new ComputeResult(Map.of(), List.of());
    }
    int vnodes = virtualNodes > 0 ? virtualNodes : 128;

    List<Point> points = new ArrayList<>(workerIds.size() * vnodes);
    for (String workerId : workerIds) {
      for (int i = 0; i < vnodes; i++) {
        int h = Murmur3_32.hash32(workerId + "#" + i, 0);
        long u = Integer.toUnsignedLong(h);
        points.add(new Point(u, workerId));
      }
    }
    points.sort(POINT_ORDER);

    Map<String, List<Range>> ranges = new LinkedHashMap<>();
    for (String wid : workerIds) {
      ranges.put(wid, new ArrayList<>());
    }

    if (workerIds.size() == 1) {
      String w = workerIds.get(0);
      ranges.get(w).add(new Range(0, UINT32_SPACE));
      return new ComputeResult(Collections.unmodifiableMap(finishRanges(ranges)), points);
    }

    int n = points.size();
    for (int i = 0; i < n; i++) {
      Point pred = points.get((i - 1 + n) % n);
      Point succ = points.get(i);
      appendKeySpaceOwnedBySuccessor(ranges.get(succ.workerId()), pred.pos(), succ.pos());
    }

    return new ComputeResult(Collections.unmodifiableMap(finishRanges(ranges)), points);
  }

  public String lookupWorker(List<String> workerIds, int virtualNodes, String key) {
    if (workerIds == null || workerIds.isEmpty()) {
      return null;
    }
    if (workerIds.size() == 1) {
      return workerIds.get(0);
    }
    int vnodes = virtualNodes > 0 ? virtualNodes : 128;

    List<Point> points = new ArrayList<>(workerIds.size() * vnodes);
    for (String workerId : workerIds) {
      for (int i = 0; i < vnodes; i++) {
        long u = Integer.toUnsignedLong(Murmur3_32.hash32(workerId + "#" + i, 0));
        points.add(new Point(u, workerId));
      }
    }
    points.sort(POINT_ORDER);

    long h = Integer.toUnsignedLong(Murmur3_32.hash32(key, 0));
    int idx = lowerBound(points, h);
    if (idx == points.size()) {
      idx = 0;
    }
    return points.get(idx).workerId();
  }

  /**
   * Keys whose {@link #lookupWorker} vnode is {@code succ} occupy (pred, succ] on the uint32 ring: unsigned interval
   * from strictly after pred through succ inclusive.
   */
  private static void appendKeySpaceOwnedBySuccessor(List<Range> out, long pred, long succ) {
    if (Long.compareUnsigned(pred, succ) == 0) {
      return;
    }
    long start = pred + 1L;
    long endInclusive = succ;
    if (Long.compareUnsigned(pred, succ) < 0) {
      long endExclusive = endInclusive + 1L;
      if (Long.compareUnsigned(start, endExclusive) < 0) {
        out.add(new Range(start, endExclusive));
      }
      return;
    }
    if (Long.compareUnsigned(start, UINT32_SPACE) < 0) {
      out.add(new Range(start, UINT32_SPACE));
    }
    if (Long.compareUnsigned(succ, UINT32_MAX) < 0) {
      long endExclusive = endInclusive + 1L;
      out.add(new Range(0L, endExclusive));
    }
  }

  private static Map<String, List<Range>> finishRanges(Map<String, List<Range>> ranges) {
    Map<String, List<Range>> merged = new LinkedHashMap<>();
    for (var e : ranges.entrySet()) {
      merged.put(e.getKey(), mergeRanges(e.getValue()));
    }
    return merged;
  }

  /** First index i with points[i].pos >= h (unsigned), or points.size() if none. */
  public static int lowerBound(List<Point> points, long h) {
    int lo = 0;
    int hi = points.size();
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      long mp = points.get(mid).pos();
      if (Long.compareUnsigned(mp, h) < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  private static List<Range> mergeRanges(List<Range> list) {
    if (list == null || list.isEmpty()) {
      return List.of();
    }
    List<Range> ranges = new ArrayList<>(list);
    ranges.sort(Comparator.comparingLong(Range::start));

    List<Range> merged = new ArrayList<>();
    Range cur = ranges.get(0);
    for (int i = 1; i < ranges.size(); i++) {
      Range nxt = ranges.get(i);
      if (cur.end() == nxt.start()) {
        cur = new Range(cur.start(), nxt.end());
      } else {
        merged.add(cur);
        cur = nxt;
      }
    }
    merged.add(cur);
    return merged;
  }

  public record Range(long start, long end) {}
  public record Point(long pos, String workerId) {}
  public record ComputeResult(Map<String, List<Range>> ranges, List<Point> points) {}
}
