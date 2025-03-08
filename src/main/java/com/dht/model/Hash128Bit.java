package com.dht.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * This class represents 128 bit integer in big endian format.
 */
@Getter
@EqualsAndHashCode
public class Hash128Bit implements Comparable<Hash128Bit>{
    private final long high;
    private final long low;

    public Hash128Bit(long high, long low) {
        this.high = high;
        this.low = low;
    }

    @Override
    public int compareTo(final Hash128Bit other) {
        if(this.high != other.high){
            return Long.compareUnsigned(this.high, other.high);
        }
        return Long.compareUnsigned(this.low, other.low);
    }
}
