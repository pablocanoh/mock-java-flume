package com.pablocano.persistance.db;

import java.util.ArrayList;
import java.util.List;

public class Cache {

    private static Cache cache = null;
    private static List<String> list = new ArrayList<>();



    public void add(String s){
        list.add(s);
    }

    public List<String> get(){
        return list;
    }

    public static synchronized Cache getInstance(){
        if (cache == null){
            cache = new Cache();
        }
        return cache;
    }
}
