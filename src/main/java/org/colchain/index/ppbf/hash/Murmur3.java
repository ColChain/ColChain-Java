package org.colchain.index.ppbf.hash;

public class Murmur3 implements IHashFunction {
    private static final long SEED = 0x7f3a21eaL;

    @Override
    public boolean isSingleValued() {
        return false;
    }

    @Override
    public long hash(byte[] bytes) {
        return com.sangupta.murmur.Murmur3.hash_x86_32(bytes, 0, SEED);
    }

    @Override
    public long[] hashMultiple(byte[] bytes) {
        return com.sangupta.murmur.Murmur3.hash_x64_128(bytes, 0, SEED);
    }
}
