package com.acme.coordination.etcd;

import io.etcd.jetcd.ByteSequence;
import java.nio.charset.StandardCharsets;

public final class EtcdBytes {
  private EtcdBytes() {}

  public static ByteSequence bs(String s) {
    return ByteSequence.from(s, StandardCharsets.UTF_8);
  }

  public static ByteSequence bb(byte[] bytes) {
    return ByteSequence.from(bytes);
  }
}

