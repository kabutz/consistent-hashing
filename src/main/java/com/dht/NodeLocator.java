package com.dht;

import com.dht.model.InstanceInfo;
import com.dht.model.RangeInstanceInfo;

import java.util.List;

public interface NodeLocator {
    InstanceInfo route(String key);
    void registerInstance(String instanceId, String host, int port);
    void deregisterInstance(String instanceId);
    List<InstanceInfo> getInstanceList();
    List<RangeInstanceInfo> getRingDetails();

}
