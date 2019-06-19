package com.pablocano.persistance;

import com.pablocano.persistance.db.Cache;

import java.util.List;
import java.util.Map;

public class MemoryPersistance implements Persintace {


    private Cache cache;

    @Override
    public void save(Object data) {
        cache.add((String) data);
    }

    @Override
    public void configure(Map<String, String> configMap) {

    }

    @Override
    public void init() {
        cache = Cache.getInstance();
    }

    public List<String> getMemory() {
        return cache.get();
    }
}
