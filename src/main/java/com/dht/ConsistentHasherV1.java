package com.dht;

import com.dht.model.*;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;


/**
 * Using Stamped lock
 */

public class ConsistentHasherV1 implements NodeLocator {
    private final boolean slowForTesting;
    private final LongAdder optimisticSuccessCount = new LongAdder();
    private final LongAdder pessimisticCount = new LongAdder();

    @Override
    public String toString() {
        return "ConsistentHasherV1{optimisticSuccessCount=" + optimisticSuccessCount +
                ", pessimisticCount=" + pessimisticCount + '}';
    }

    private static final int OPTIMISTIC_RETRY_CNT = 3;
    private final StampedLock stampedLock = new StampedLock();
    private static final int VIRTUAL_NODE_CNT = 420;
    private static final HashFunction DEFAULT_HASH_FN = Hashing.murmur3_128();
    private final HashFunction hashFunction;
    private final NavigableMap<Hash128Bit, VirtualNode> hashRing = new TreeMap<>();
    private final Map<String, InstanceInfoHashRange<Hash128Bit[]>> instanceIdToVNodeHashes = new HashMap<>();

    public ConsistentHasherV1() {
        this(false);
    }

    public ConsistentHasherV1(boolean slowForTesting) {
        this(null, slowForTesting);
    }

    public ConsistentHasherV1(final HashFunction hashFunction, boolean slowForTesting) {
        this.hashFunction = Objects.isNull(hashFunction) ? DEFAULT_HASH_FN : hashFunction;
        this.slowForTesting = slowForTesting;
    }

    @Override
    public InstanceInfo route(final String key) {
        byte[] bytes = hashFunction.hashString(key, StandardCharsets.UTF_8)
                .asBytes();
        Hash128Bit hash128Bit = getHash128Bit(bytes);
        // first trying with Optimistic locking
        for (int ctr = 0; ctr < OPTIMISTIC_RETRY_CNT; ctr++) {
            long stamp = stampedLock.tryOptimisticRead();
            InstanceInfo instanceInfo = getInstanceInfo(hash128Bit);
            if (stampedLock.validate(stamp)) {
                optimisticSuccessCount.increment();
                return instanceInfo;
            }
        }

        // Now trying with PESSIMISTIC locking, blocking call
        long stamp = stampedLock.readLock();
        try {
            pessimisticCount.increment();
            return getInstanceInfo(hash128Bit);
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public void registerInstance(final String instanceId, final String host, final int port) {
        long stamp = stampedLock.writeLock();
        try {
            if (instanceIdToVNodeHashes.containsKey(instanceId)) {
                return;
            }
            InstanceInfo instanceInfo = new InstanceInfo(instanceId, host, port);

            Hash128Bit[] vNodeHashes = new Hash128Bit[VIRTUAL_NODE_CNT];
            for (int ctr = 0; ctr < VIRTUAL_NODE_CNT; ctr++) {
                VirtualNode virtualNode = new VirtualNode(instanceInfo, ctr);
                byte[] bytes = hashFunction.hashString(virtualNode.getKey(), StandardCharsets.UTF_8)
                        .asBytes();
                Hash128Bit hash128Bit = getHash128Bit(bytes);
                vNodeHashes[ctr] = hash128Bit;
                hashRing.put(hash128Bit, virtualNode);
            }
            instanceIdToVNodeHashes.put(instanceId, new InstanceInfoHashRange(instanceInfo, vNodeHashes));
        } finally {
            stampedLock.unlock(stamp);
        }
    }

    @Override
    public void deregisterInstance(final String instanceId) {
        long stamp = stampedLock.writeLock();
        try {
            if (!instanceIdToVNodeHashes.containsKey(instanceId)) {
                return;
            }
            InstanceInfoHashRange<Hash128Bit[]> instanceInfoHashRange = instanceIdToVNodeHashes.remove(instanceId);
            for (Hash128Bit vNodeHash : instanceInfoHashRange.vNodeHashArr()) {
                hashRing.remove(vNodeHash);
            }
        } finally {
            stampedLock.unlock(stamp);
        }
    }

    @Override
    public List<InstanceInfo> getInstanceList() {
        long stamp = stampedLock.readLock();
        try {
            List<InstanceInfo> instanceInfoList = new ArrayList<>(this.instanceIdToVNodeHashes.size());
            for (Entry<String, InstanceInfoHashRange<Hash128Bit[]>> entry : this.instanceIdToVNodeHashes.entrySet()) {
                InstanceInfoHashRange instanceInfoHashRange = entry.getValue();
                instanceInfoList.add(instanceInfoHashRange.instanceInfo());
            }
            return instanceInfoList;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public List<RangeInstanceInfo> getRingDetails() {
        long stamp = stampedLock.readLock();
        try {
            List<RangeInstanceInfo> rangeInstanceInfoList = new ArrayList<>(this.instanceIdToVNodeHashes.size());
            for (Entry<String, InstanceInfoHashRange<Hash128Bit[]>> entry : this.instanceIdToVNodeHashes.entrySet()) {
                InstanceInfoHashRange<Hash128Bit[]> instanceInfoHashRange = entry.getValue();
                Hash128Bit[] vNodeHashArr = instanceInfoHashRange.vNodeHashArr();
                Hash128Bit startHash = vNodeHashArr[0];
                Hash128Bit endHash = vNodeHashArr[vNodeHashArr.length - 1];
                rangeInstanceInfoList.add(
                        new RangeInstanceInfo(startHash, endHash, instanceInfoHashRange.instanceInfo()));
            }
            return rangeInstanceInfoList;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    private InstanceInfo getInstanceInfo(final Hash128Bit hash128Bit) {
        // temp change for high, low
        Entry<Hash128Bit, VirtualNode> entry = this.hashRing.ceilingEntry(hash128Bit);
        if (Objects.nonNull(entry)) {
            return entry.getValue().instanceInfo();
        }
        return this.hashRing.isEmpty() ? null : getFirstEntryInstanceInfo();
    }

    private InstanceInfo getFirstEntryInstanceInfo() {
        if (slowForTesting) LockSupport.parkNanos(50_000_000);
        return this.hashRing.firstEntry()
                .getValue()
                .instanceInfo();
    }

    private Hash128Bit getHash128Bit(final byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        long high = buffer.getLong();
        long low = buffer.getLong();
        Hash128Bit hash128Bit = new Hash128Bit(high, low);
        return hash128Bit;
    }
}
