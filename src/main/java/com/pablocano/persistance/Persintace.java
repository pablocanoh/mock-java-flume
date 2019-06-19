package com.pablocano.persistance;

import java.util.Map;

public interface Persintace {

    void save(Object data);

    void configure(Map<String, String> configMap);

    void init();
}
