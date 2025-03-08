package com.dht;

import com.dht.model.InstanceInfo;
import com.dht.model.RangeInstanceInfo;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHasherV1Test {

    private NodeLocator nodeLocator;

    private static final ThreadMXBean tmxBean = ManagementFactory.getThreadMXBean();

    @BeforeEach
    void setUp() {
        nodeLocator = new ConsistentHasherV1();
        //nodeLocator = new ConsistentHasherV2();
    }

    @Test
    void test_register() {
        nodeLocator.registerInstance("instance1", "host1", 8080);
        List<InstanceInfo> instances = nodeLocator.getInstanceList();
        assertEquals(1, instances.size());
        assertEquals("instance1", instances.get(0).getInstanceId());
        assertEquals("host1", instances.get(0).getHost());
        assertEquals(8080, instances.get(0).getPort());
    }

    @Test
    void test_deregister() {
        nodeLocator.registerInstance("instance1", "host1", 8080);
        nodeLocator.deregisterInstance("instance1");
        List<InstanceInfo> instances = nodeLocator.getInstanceList();
        assertTrue(instances.isEmpty());
    }

    @Test
    void test_getRingDetails_empty() {
        List<RangeInstanceInfo> ringDetails = nodeLocator.getRingDetails();
        assertTrue(ringDetails.isEmpty());
    }

    @Test
    void test_routeWithNoInstances() {
        val instance = nodeLocator.route("key1");
        assertNull(instance);
    }

    @Test
    void test_routeWithSingleInstance() {
        nodeLocator.registerInstance("instance1", "host1", 8080);
        val instance = nodeLocator.route("key1");
        assertEquals("instance1", instance.getInstanceId());
    }

    @Test
    void test_routeWithTwoInstances() {
        nodeLocator.registerInstance("instance1", "host1", 8080);
        nodeLocator.registerInstance("instance2", "host2", 8081);
        val instance = nodeLocator.route("key1");
        assertTrue(instance.getInstanceId().equals("instance1") || instance.getInstanceId().equals("instance2"));

        val instance2 = nodeLocator.route("key2"); // Potentially different key, might route to a different instance
        assertTrue(instance2.getInstanceId().equals("instance1") || instance2.getInstanceId().equals("instance2"));
    }

    @Test
    void test_multipleInstances_equalDistribution() {
        int instanceCount = 3;
        long requestCount = 100_000;
        registerInstances(nodeLocator, instanceCount);

        val routeCountsMap = generateLoad(nodeLocator, requestCount);

        // Assert reasonably equal distribution (allowing some variance)
        double minExpectedCount = (double) requestCount / instanceCount * 0.9;
        for (int i = 0; i < instanceCount; i++) {
            assertTrue(routeCountsMap.get("instance" + i) > minExpectedCount);
        }
    }

    @Test
    void test_newInstanceRegistration_equalDistribution() {
        int instanceCount = 10;
        long requestCount = 100_000;
        registerInstances(nodeLocator, instanceCount);

        val routeCountsMap = generateLoad(nodeLocator, requestCount);

        // Assert reasonably equal distribution (allowing some variance)
        double minExpectedCount = (double) requestCount / instanceCount * 0.9;
        for (int i = 0; i < instanceCount; i++) {
            assertTrue(routeCountsMap.get("instance" + i) > minExpectedCount);
        }

        // Now add one node and ensure that all nodes get equal traffic.
        registerInstance(nodeLocator, 10);
        assertEquals(nodeLocator.getInstanceList().size(), 11);

        val newRouteCountsMap = generateLoad(nodeLocator, requestCount);

        minExpectedCount = (double) requestCount / (instanceCount + 1) * 0.9;
        for (int i = 0; i < instanceCount + 1; i++) {
            assertTrue(newRouteCountsMap.get("instance" + i) > minExpectedCount);
        }
    }

    @Test
    void test_instanceDeregistration_equalDistributionOtherNodes() {
        int instanceCount = 10;
        long requestCount = 100_000;
        registerInstances(nodeLocator, instanceCount);

        val routeCountsMap = generateLoad(nodeLocator, requestCount);

        // Assert reasonably equal distribution (allowing some variance)
        double minExpectedCount = (double) requestCount / instanceCount * 0.9;
        for (int i = 0; i < instanceCount; i++) {
            assertTrue(routeCountsMap.get("instance" + i) > minExpectedCount);
        }

        // Now remove one node and ensure that all nodes get equal traffic.
        nodeLocator.deregisterInstance("instance9");
        assertEquals(nodeLocator.getInstanceList().size(), 9);

        val newRouteCountsMap = generateLoad(nodeLocator, requestCount);

        minExpectedCount = (double) requestCount / (instanceCount - 1) * 0.9;
        for (int i = 0; i < instanceCount - 1; i++) {
            assertTrue(newRouteCountsMap.get("instance" + i) > minExpectedCount);
        }
    }

    static void registerInstance(NodeLocator nodeLocator, int nodeId) {
        nodeLocator.registerInstance("instance" + nodeId, "host" + nodeId, 8080);
    }

    static Map<String, Integer> generateLoad(NodeLocator nodeLocator, long requestCount) {
        Map<String, Integer> routeCounts = new HashMap<>();
        for (int i = 0; i < requestCount; i++) {
            val instance = nodeLocator.route("key" + i);
            String instanceId = instance.getInstanceId();
            routeCounts.put(instanceId, routeCounts.getOrDefault(instanceId, 0) + 1);
        }
        return routeCounts;
    }
    
    @Test
    void testRouteUsingMultipleThreads() throws InterruptedException {

        long start = System.currentTimeMillis();
        int instanceCount = 10;
        int requestCount = 1_000_000;
        registerInstances(nodeLocator, instanceCount);


        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Thread thread = new Thread(() -> {
                long time = System.currentTimeMillis();
                for (int ctr = 0; ctr < requestCount; ctr++) {
                    InstanceInfo instance = nodeLocator.route("key" + ctr);
                }
                time = System.currentTimeMillis() - time;
                System.out.printf("route() called %,d times, elapsed=%d", requestCount,  time);
                System.out.println();
            }, "ThreadId:"+i);

            threadList.add(thread);

            thread.start();

        }

        for (Thread thread : threadList) {
            thread.join();
        }
    }

    static void registerInstances(NodeLocator NodeLocator, int instanceCount) {
        for (int i = 0; i < instanceCount; i++) {
            registerInstance(NodeLocator, i);
        }
    }

}