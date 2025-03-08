package com.dht.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class InstanceInfo {
    private final String instanceId;
    private final String host;
    private final int port;

    public String getKey() {
        return instanceId + "_" + host + "_" + port;
    }
}
