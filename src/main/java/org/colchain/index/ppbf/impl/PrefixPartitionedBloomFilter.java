package org.colchain.index.ppbf.impl;

import org.colchain.index.graph.IGraph;
import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.ppbf.hash.IHashFunction;
import org.colchain.index.ppbf.hash.Murmur3;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.colchain.colchain.node.AbstractNode;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PrefixPartitionedBloomFilter implements IBloomFilter<String> {
    private final IHashFunction hash;
    private Map<String, Long> partitions = new HashMap<>();
    private int numBits;
    private int numHashFunctions;
    private long numInsertedElements;
    private String filename;
    private long currentOffset;
    private IGraph graph;
    private boolean empty = true;
    private boolean emptyChecked = false;

    private PrefixPartitionedBloomFilter(long numElements, double maxFpp, String file) {
        this.hash = new Murmur3();
        this.filename = file;

        File f = new File(filename);
        File f1 = new File(filename + "p");

        if (!f.exists() || !f1.exists()) {
            this.numBits = numBits(numElements, maxFpp);
            this.numHashFunctions = numHashFunctions(numElements, this.numBits);
            deleteFile();
            System.out.println("Building Bloom Filter for "+file);
            try {
                f.createNewFile();
                f1.createNewFile();
            } catch (IOException e) {
            }
            try {
                RandomAccessFile raf = new RandomAccessFile(filename, "rw");
                raf.seek(0);
                raf.writeInt(numBits);

                raf.seek(4);
                raf.writeInt(numHashFunctions);

                raf.seek(8);
                raf.writeLong(numInsertedElements);
                raf.close();
            } catch (IOException e) {
            }
            currentOffset = 16;
        } else {
            readPartitions(f1.getAbsolutePath());
            try {
                RandomAccessFile raf = new RandomAccessFile(filename, "rw");
                raf.seek(0);
                numBits = raf.readInt();

                raf.seek(4);
                numHashFunctions = raf.readInt();

                raf.seek(8);
                numInsertedElements = raf.readLong();
                raf.close();
            } catch (IOException e) {
                numBits = 0;
                numHashFunctions = 0;
            }
            currentOffset = 16 + (partitions.size() * numBits);
        }

    }



    public void setFilename(String filename) {
        this.filename = filename;
    }

    private PrefixPartitionedBloomFilter(int numBits, int numHash, Map<String, Long> partitions, String file) {
        this.hash = new Murmur3();
        this.numBits = numBits;
        this.numHashFunctions = numHash;
        this.partitions = partitions;
        this.filename = file;
        currentOffset = 16 + (partitions.size() * numBits);
    }

    private PrefixPartitionedBloomFilter() {
        this.hash = new Murmur3();
        this.numBits = 0;
        this.numHashFunctions = 0;
        this.filename = "";
    }

    private void readPartitions(String file) {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine();
            while (line != null) {
                String[] words = line.split(";;");
                partitions.put(words[0], Long.parseLong(words[1]));
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
        }
    }

    public String getFilename() {
        return filename;
    }

    public static PrefixPartitionedBloomFilter create(long numElements, double maxFpp, String file) {
        return new PrefixPartitionedBloomFilter(numElements, maxFpp, file);
    }

    public static PrefixPartitionedBloomFilter create(double maxFpp, String file) {
        return new PrefixPartitionedBloomFilter(50000, maxFpp, file);
    }

    public static PrefixPartitionedBloomFilter create(String file) {
        return new PrefixPartitionedBloomFilter(50000, 0.1, file);
    }

    public static PrefixPartitionedBloomFilter empty() {
        return new PrefixPartitionedBloomFilter();
    }

    public Map<String, Long> getPartitions() {
        return partitions;
    }

    public IGraph getGraph() {
        return graph;
    }

    public void setGraph(IGraph graph) {
        this.graph = graph;
    }

    @Override
    public long elementCount() {
        return numInsertedElements;
    }

    @Override
    public boolean mightContain(String element) {
        String prefix = getPrefix(element);
        if (!partitions.containsKey(prefix)) return false;

        long offset = partitions.get(prefix);
        String name = element.replace(prefix, "");

        try {
            return contains(name.getBytes(), offset);
        } catch (IOException e) {
            return false;
        }
    }

    private String getPrefix(String uri) {
        if(uri.endsWith("/")) {
            uri = uri.substring(0, uri.length()-1);
        }
        int count = StringUtils.countMatches(uri, "/")-2;
        if(count == 0) return "N/A";
        if(count <= 4) {
            if(count < 1) System.out.println(uri);
            return uri.substring(0, StringUtils.ordinalIndexOf(uri, "/", 3));
        }

        int no = 2 + (int)Math.ceil((double)count / 2.0);

        int index = StringUtils.ordinalIndexOf(uri, "/", no);

        try {
            return uri.substring(0, index);
        } catch (StringIndexOutOfBoundsException e) {
            return uri.substring(0, uri.lastIndexOf("/"));
        }
    }

    public void showPrefix(String element) {
        String prefix = getPrefix(element);
        String name = element.replace(prefix, "");
        System.out.println(element);
        System.out.println(prefix);
        System.out.println(name);
    }



    @Override
    public void put(String element) {
        String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        if(!element.matches(regex)) return;

        //showPrefix(element);
        String prefix = getPrefix(element);
        String name = element.replace(prefix, "");
        numInsertedElements++;

        try {
            RandomAccessFile raf = new RandomAccessFile(filename, "rw");

            raf.seek(8);
            raf.writeLong(numInsertedElements);

            if (!partitions.containsKey(prefix)) {
                partitions.put(prefix, currentOffset);
                long offset = currentOffset;
                Writer output = new BufferedWriter(new FileWriter(filename + "p", true));
                output.append(prefix + ";;" + offset + "\n");
                output.close();

                currentOffset += getNumBytes(numBits);

                while (offset < currentOffset) {
                    raf.seek(offset);
                    raf.writeByte(0);
                    offset++;
                }
            }
            raf.close();
            put(name.getBytes(), partitions.get(prefix));
        } catch (IOException e) {
        }
    }

    @Override
    public IBloomFilter<String> intersect(IBloomFilter<String> other) {
        PrefixPartitionedBloomFilter ppbf = (PrefixPartitionedBloomFilter) other;
        String newFilename = AbstractNode.getState().getDatastore() +  "index/" + filename.substring(filename.lastIndexOf("/") + 1).replace(".hdt.ppbf", "") + "-" + ppbf.filename.substring(ppbf.filename.lastIndexOf("/") + 1).replace(".hdt.ppbf", "") + ".ppbf";

        File f = new File(newFilename);
        if(f.exists()) return create(f.getAbsolutePath());

        File f1 = new File(newFilename + "p");
        if (!f.exists()) {
            try {
                f.createNewFile();
                f1.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Map<String, Long> part = new HashMap<>();
        try {
            RandomAccessFile raf = new RandomAccessFile(newFilename, "rw");
            RandomAccessFile raf1 = new RandomAccessFile(filename, "r");
            RandomAccessFile raf2;
            try {
                raf2 = new RandomAccessFile(ppbf.filename, "r");
            } catch (FileNotFoundException e) {
                System.out.println("File not found " + ppbf.filename);
                return empty();
            }
            raf.seek(0);
            raf.writeInt(numBits);

            raf.seek(4);
            raf.writeInt(numHashFunctions);

            raf.seek(8);
            raf.writeLong(numInsertedElements);

            long offset = 16;
            for (Map.Entry<String, Long> entry : partitions.entrySet()) {
                if (!ppbf.partitions.containsKey(entry.getKey())) continue;
                long newoffset = offset + getNumBytes(numBits);
                long of1 = entry.getValue();
                long of2 = ppbf.partitions.get(entry.getKey());

                Writer output = new BufferedWriter(new FileWriter(newFilename + "p", true));
                output.append(entry.getKey() + ";;" + offset + "\n");
                output.close();

                part.put(entry.getKey(), offset);

                while (offset < newoffset) {
                    raf.seek(offset);
                    raf1.seek(of1);
                    raf2.seek(of2);

                    raf.writeByte(raf1.readByte() & raf2.readByte());
                    offset++;
                    of1++;
                    of2++;
                }
            }

            raf.close();
            raf1.close();
            raf2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new PrefixPartitionedBloomFilter(numBits, numHashFunctions, part, newFilename);
    }

    @Override
    public IBloomFilter<String> copy() {
        return new PrefixPartitionedBloomFilter(numBits, numHashFunctions, partitions, filename);
    }

    @Override
    public void clear() {
        partitions = new HashMap<>();

    }

    @Override
    public boolean isEmpty() {
        if(emptyChecked) return empty;
        if (partitions.size() == 0) return true;

        for (long o : partitions.values()) {
            try {
                if (!isEmpty(o)) {
                    empty = false;
                    emptyChecked = true;
                    return false;
                }
            } catch (IOException e) {
                empty = false;
                emptyChecked = true;
                return false;
            }
        }

        partitions.clear();
        empty = true;
        emptyChecked = true;
        return true;
    }

    @Override
    public void deleteFile() {
        File f = new File(filename);
        File f1 = new File(filename + "p");
        f.delete();
        f1.delete();
    }

    private boolean isEmpty(long off) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filename, "r");

        long maxoff = off + getNumBytes(numBits);
        long offset = off;

        while (offset < maxoff) {
            raf.seek(offset);
            byte b = raf.readByte();
            if (b != 0) {
                raf.close();
                return false;
            }

            offset++;
        }

        raf.close();
        return true;
    }

    private boolean contains(byte[] bytes, long offset) throws IOException {
        long hash64 = getLongHash64(bytes);

        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        for (int i = 1; i <= this.numHashFunctions; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            int nh = nextHash % numBits;
            long newoff = Math.floorDiv(nh, 8) + offset;
            raf.seek(newoff);
            byte b1 = raf.readByte();

            int mod = nh % 8;
            byte b2;
            if (mod == 0) {
                b2 = -128;
            } else if (mod == 1) {
                b2 = 64;
            } else if (mod == 2) {
                b2 = 32;
            } else if (mod == 3) {
                b2 = 16;
            } else if (mod == 4) {
                b2 = 8;
            } else if (mod == 5) {
                b2 = 4;
            } else if (mod == 6) {
                b2 = 2;
            } else {
                b2 = 1;
            }
            if ((b1 & b2) != b2) {
                raf.close();
                return false;
            }
        }
        raf.close();
        return true;
    }

    private void put(byte[] bytes, long offset) throws IOException {
        long hash64 = getLongHash64(bytes);
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        RandomAccessFile raf = new RandomAccessFile(filename, "rw");
        for (int i = 1; i <= this.numHashFunctions; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            int nh = nextHash % numBits;
            long newoff = Math.floorDiv(nh, 8) + offset;
            raf.seek(newoff);
            byte b1 = raf.readByte();

            int mod = nh % 8;
            byte b2;
            if (mod == 0) {
                b2 = -128;
            } else if (mod == 1) {
                b2 = 64;
            } else if (mod == 2) {
                b2 = 32;
            } else if (mod == 3) {
                b2 = 16;
            } else if (mod == 4) {
                b2 = 8;
            } else if (mod == 5) {
                b2 = 4;
            } else if (mod == 6) {
                b2 = 2;
            } else {
                b2 = 1;
            }

            byte b = (byte) (b1 | b2);

            raf.seek(newoff);
            raf.writeByte(b);
        }
        raf.close();
    }

    private static String getFileNameWithoutExtension(File file) {
        String fileName = "";

        try {
            String name = file.getName();
            fileName = name.replaceFirst("[.][^.]+$", "");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return fileName;

    }

    private long getLongHash64(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Bytes to add to bloom filter cannot be null");
        }

        if (this.hash.isSingleValued()) {
            return this.hash.hash(bytes);
        }

        return this.hash.hashMultiple(bytes)[0];
    }

    private static int numBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int numHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    private static int getNumBytes(int numBits) {
        int num = 0;
        if (!((numBits % 8) == 0))
            num = 1;

        num += (numBits / 8);
        return num;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrefixPartitionedBloomFilter that = (PrefixPartitionedBloomFilter) o;
        return Objects.equals(hash, that.hash) &&
                Objects.equals(partitions, that.partitions) &&
                Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, partitions, filename);
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.toJson(this);
    }

    public static PrefixPartitionedBloomFilter fromString(String str) {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.fromJson(str, PrefixPartitionedBloomFilter.class);
    }
}
