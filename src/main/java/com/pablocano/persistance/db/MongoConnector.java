package com.pablocano.persistance.db;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;


public class MongoConnector {

    private static MongoConnector instance;
    private static MongoClient mongoClient;
    private static MongoDatabase database;


    public static synchronized MongoConnector getInstance() {
        if (instance == null){
            instance = new MongoConnector();
            Runtime.getRuntime().addShutdownHook(new Thread(instance::stop));
        }

        return instance;
    }


    public void init(String host, int port){
        mongoClient = new MongoClient(host, port);
        database = mongoClient.getDatabase("myMongoDb");
    }

    private void stop(){
        if (mongoClient != null){
            mongoClient.close();
        }
    }

    public List<Document> getAll(){
        List<Document> results = new ArrayList<>();
        MongoCollection<Document> collection = database.getCollection("test-collection");
        for (Document cur : collection.find()) {
            results.add(cur);
        }

        return results;
    }

    public void insert(String document){
        MongoCollection<Document> collection = database.getCollection("test-collection");
        collection.insertOne(Document.parse(document));
    }
}
