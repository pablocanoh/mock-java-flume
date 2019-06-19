package com.pablocano.persistance;

import com.pablocano.persistance.db.MongoConnector;

import java.util.Map;

public class MongoPersistance implements Persintace {

    private String host;
    private String port;

    @Override
    public void save(Object data) {

        MongoConnector.getInstance().insert((String) data);
    }

    @Override
    public void configure(Map<String, String> configMap) {
        host = configMap.get("host-mongo");
        port = configMap.get("port-mongo");

    }

    @Override
    public void init() {
        MongoConnector.getInstance().init(host, Integer.parseInt(port));
    }
}
