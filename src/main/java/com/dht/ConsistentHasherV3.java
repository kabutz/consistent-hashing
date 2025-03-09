package com.dht;

import com.dht.model.*;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Using ConcurrentSkipListMap
 */

@ThreadSafe
public class ConsistentHasherV3 implements NodeLocator {
    private static final int VIRTUAL_NODE_CNT = 420;
    private static final HashFunction DEFAULT_HASH_FN = Hashing.murmur3_128();
    private final HashFunction hashFunction;
    private final NavigableMap<Hash128Bit, VirtualNode> hashRing = new ConcurrentSkipListMap<>();
    private final Map<String, InstanceInfoHashRange<Hash128Bit[]>> instanceIdToVNodeHashes = new ConcurrentHashMap<>();

    public ConsistentHasherV3() {
        this(null);
    }

    public ConsistentHasherV3(HashFunction hashFunction) {
        this.hashFunction = Objects.isNull(hashFunction) ? DEFAULT_HASH_FN : hashFunction;
    }

    @Override
    public InstanceInfo route(final String key) {
        byte[] bytes = hashFunction.hashString(key, StandardCharsets.UTF_8)
                .asBytes();
        return getInstanceInfo(getHash128Bit(bytes));
    }

    @Override
    public void registerInstance(final String instanceId,
                                 final String host, final int port) {
        instanceIdToVNodeHashes.computeIfAbsent(instanceId, key -> {
            InstanceInfo instanceInfo = new InstanceInfo(key, host, port);

            Hash128Bit[] vNodeHashes = new Hash128Bit[VIRTUAL_NODE_CNT];
            for (int ctr = 0; ctr < VIRTUAL_NODE_CNT; ctr++) {
                VirtualNode virtualNode = new VirtualNode(instanceInfo, ctr);
                byte[] bytes = hashFunction.hashString(virtualNode.getKey(), StandardCharsets.UTF_8)
                        .asBytes();
                Hash128Bit hash128Bit = getHash128Bit(bytes);
                vNodeHashes[ctr] = hash128Bit;
                hashRing.put(hash128Bit, virtualNode);
            }
            return new InstanceInfoHashRange<>(instanceInfo, vNodeHashes);
        });
    }

    @Override
    public void deregisterInstance(final String instanceId) {
        instanceIdToVNodeHashes.computeIfPresent(instanceId, (key, instanceInfoHashRange) -> {
            for (Hash128Bit vNodeHash : instanceInfoHashRange.vNodeHashArr()) {
                hashRing.remove(vNodeHash);
            }
            return null; // remove
        });
    }

    @Override
    public List<InstanceInfo> getInstanceList() {
        return instanceIdToVNodeHashes.values()
                .stream()
                .map(InstanceInfoHashRange::instanceInfo)
                .toList();
    }

    @Override
    public List<RangeInstanceInfo> getRingDetails() {
        return instanceIdToVNodeHashes.values().stream()
                .map(hashRange -> {
                    Hash128Bit[] vNodeHashArr = hashRange.vNodeHashArr();
                    Hash128Bit startHash = vNodeHashArr[0];
                    Hash128Bit endHash = vNodeHashArr[vNodeHashArr.length - 1];
                    return new RangeInstanceInfo(startHash, endHash, hashRange.instanceInfo());
                })
                .toList();
    }

    private InstanceInfo getInstanceInfo(final Hash128Bit hash128Bit) {
        // temp change for high, low
        Entry<Hash128Bit, VirtualNode> entry = this.hashRing.ceilingEntry(hash128Bit);
        if (Objects.nonNull(entry)) {
            return entry.getValue().instanceInfo();
        }
        return this.hashRing.isEmpty() ? null : this.hashRing.firstEntry()
                .getValue()
                .instanceInfo();
    }

    private Hash128Bit getHash128Bit(final byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN);
        long high = buffer.getLong();
        long low = buffer.getLong();
        Hash128Bit hash128Bit = new Hash128Bit(high, low);
        return hash128Bit;
    }
}
