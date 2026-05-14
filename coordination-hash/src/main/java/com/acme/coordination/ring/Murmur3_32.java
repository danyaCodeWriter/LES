package com.acme.coordination.ring;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public final class Murmur3_32 {
  private Murmur3_32() {}

  public static int hash32(String input, int seed) {
    byte[] data = input.getBytes(StandardCharsets.UTF_8);
    return hash32(data, seed);
  }

  public static int hash32(byte[] data, int seed) {
    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;
    ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

    while (buffer.remaining() >= 4) {
      int k1 = buffer.getInt();
      k1 *= c1;
      k1 = Integer.rotateLeft(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = Integer.rotateLeft(h1, 13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    int k1 = 0;
    int remaining = buffer.remaining();
    if (remaining == 3) {
      k1 ^= (buffer.get(2) & 0xff) << 16;
    }
    if (remaining >= 2) {
      k1 ^= (buffer.get(1) & 0xff) << 8;
    }
    if (remaining >= 1) {
      k1 ^= (buffer.get(0) & 0xff);
      k1 *= c1;
      k1 = Integer.rotateLeft(k1, 15);
      k1 *= c2;
      h1 ^= k1;
    }

    h1 ^= data.length;
    h1 = fmix(h1);
    return h1;
  }

  private static int fmix(int h) {
    h ^= (h >>> 16);
    h *= 0x85ebca6b;
    h ^= (h >>> 13);
    h *= 0xc2b2ae35;
    h ^= (h >>> 16);
    return h;
  }
}
