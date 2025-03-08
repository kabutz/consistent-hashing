package com.dht.model;

public record VirtualNode(InstanceInfo instanceInfo, int vNodeIdx) {

    public String getKey(){
        return this.instanceInfo.getKey() + this.vNodeIdx;
    }
}
