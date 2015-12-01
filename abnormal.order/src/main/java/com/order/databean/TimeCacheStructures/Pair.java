package com.order.databean.TimeCacheStructures;

import java.io.Serializable;

/**
 * 设计用于存储
 * SessionId SessionInfo
 * UserId    UserInfo
 * 这样的信息对。
 * <p/>
 * Created by LiMingji on 2015/5/27.
 */
public class Pair<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;
    private K key;
    private V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return key.equals(((Pair) obj).getKey());
    }

    @Override
    public String toString() {
        return "key=" + key + ";value=" + value + ". ";
    }
}
