package org.colchain.index.ppbf;

public interface IBloomFilter<T> {
    long elementCount();

    boolean mightContain(T element);

    void put(T element);

    IBloomFilter<T> intersect(IBloomFilter<T> other);

    IBloomFilter<T> copy();

    void clear();

    boolean isEmpty();

    void deleteFile();
}
