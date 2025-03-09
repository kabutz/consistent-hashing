package com.dht;

import com.dht.model.Hash128Bit;
import com.dht.model.InstanceInfo;
import com.dht.model.InstanceInfoHashRange;
import com.dht.model.RangeInstanceInfo;
import com.dht.model.VirtualNode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Using ReentrantReadWriteLock
 */

@ThreadSafe
public class ConsistentHasherV2 implements NodeLocator {

    private static final int OPTIMISTIC_RETRY_CNT = 3;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final int VIRTUAL_NODE_CNT = 420;
    private static final HashFunction DEFAULT_HASH_FN = Hashing.murmur3_128();
    private final HashFunction hashFunction;
    private final NavigableMap<Hash128Bit, VirtualNode> hashRing = new TreeMap<>();
    private final Map<String, InstanceInfoHashRange<Hash128Bit[]>> instanceIdToVNodeHashes = new HashMap<>();

    public ConsistentHasherV2() {
        this(null);
    }

    public ConsistentHasherV2(final HashFunction hashFunction) {
        this.hashFunction = Objects.isNull(hashFunction) ? DEFAULT_HASH_FN : hashFunction;
    }

    @Override
    public InstanceInfo route(final String key) {
        byte[] bytes = hashFunction.hashString(key, StandardCharsets.UTF_8).asBytes();
        Hash128Bit hash128Bit = getHash128Bit(bytes);
        readWriteLock.readLock().lock();
        //readWriteLock.writeLock().lock();
        try {
            return getInstanceInfo(hash128Bit);
        } finally {
            readWriteLock.readLock().unlock();
            //readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void registerInstance(final String instanceId, final String host, final int port) {
        readWriteLock.writeLock().lock();
        try {
            if (instanceIdToVNodeHashes.containsKey(instanceId)) {
                return;
            }
            InstanceInfo instanceInfo = new InstanceInfo(instanceId, host, port);

            Hash128Bit[] vNodeHashes = new Hash128Bit[VIRTUAL_NODE_CNT];
            for (int ctr = 0; ctr < VIRTUAL_NODE_CNT; ctr++) {
                VirtualNode virtualNode = new VirtualNode(instanceInfo, ctr);
                byte[] bytes = hashFunction.hashString(virtualNode.getKey(), StandardCharsets.UTF_8).asBytes();
                Hash128Bit hash128Bit = getHash128Bit(bytes);
                vNodeHashes[ctr] = hash128Bit;
                hashRing.put(hash128Bit, virtualNode);
            }
            instanceIdToVNodeHashes.put(instanceId, new InstanceInfoHashRange(instanceInfo, vNodeHashes));
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void deregisterInstance(final String instanceId) {
        readWriteLock.writeLock().lock();
        try {
            if (!instanceIdToVNodeHashes.containsKey(instanceId)) {
                return;
            }
            InstanceInfoHashRange<Hash128Bit[]> instanceInfoHashRange = instanceIdToVNodeHashes.remove(instanceId);
            for (Hash128Bit vNodeHash : instanceInfoHashRange.vNodeHashArr()) {
                hashRing.remove(vNodeHash);
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public List<InstanceInfo> getInstanceList() {
        readWriteLock.writeLock().lock();
        try {
            List<InstanceInfo> instanceInfoList = new ArrayList<>(this.instanceIdToVNodeHashes.size());
            for (Entry<String, InstanceInfoHashRange<Hash128Bit[]>> entry : this.instanceIdToVNodeHashes.entrySet()) {
                InstanceInfoHashRange instanceInfoHashRange = entry.getValue();
                instanceInfoList.add(instanceInfoHashRange.instanceInfo());
            }
            return instanceInfoList;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public List<RangeInstanceInfo> getRingDetails() {
        readWriteLock.readLock().lock();
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
            readWriteLock.readLock().lock();
        }
    }


    private InstanceInfo getInstanceInfo(final Hash128Bit hash128Bit) {
        //temp change for high, low
        Entry<Hash128Bit, VirtualNode> entry = this.hashRing.ceilingEntry(hash128Bit);
        if (Objects.nonNull(entry)) {
            return entry.getValue().instanceInfo();
        }
        return this.hashRing.isEmpty() ? null : this.hashRing.firstEntry().getValue().instanceInfo();
    }

    private Hash128Bit getHash128Bit(final byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        long high = buffer.getLong();
        long low = buffer.getLong();
        Hash128Bit hash128Bit = new Hash128Bit(high, low);
        return hash128Bit;
    }
}
