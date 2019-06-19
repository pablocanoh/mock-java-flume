package com.pablocano.sink;

import com.pablocano.persistance.db.Cache;
import com.pablocano.persistance.db.MongoConnector;
import com.pablocano.util.MongoEmbeded;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PersintanceSinkTest {

    private static CustomSink memorySink;
    private static CustomSink mongoSink;
    private static Channel channel;

    @BeforeClass
    public static void beforeClass() throws IOException {

        new MongoEmbeded();

        channel = new MemoryChannel();
        Context channelContext = new Context();
        Configurables.configure(channel, channelContext);

        memorySink = new PersintanceSink();
        mongoSink = new PersintanceSink();

        Context memorySinkContext = new Context();
        memorySinkContext.put("persistence-class-name", "com.pablocano.persistance.MemoryPersistance");

        Context mongoSinkContext = new Context();
        mongoSinkContext.put("persistence-class-name", "com.pablocano.persistance.MongoPersistance");
        mongoSinkContext.put("host-mongo", "localhost");
        mongoSinkContext.put("port-mongo", "12345");

        Configurables.configure(memorySink, memorySinkContext);
        memorySink.setChannel(channel);

        Configurables.configure(mongoSink, mongoSinkContext);
        mongoSink.setChannel(channel);

        channel.start();
        memorySink.start();
        mongoSink.start();
    }

    @AfterClass
    public static void afterClass(){
        memorySink.stop();
        mongoSink.stop();
        channel.stop();
    }

    @Test
    public void memoryTest(){
        putEventInChannel("hello");
        putEventInChannel("world");
        memorySink.process();
        memorySink.process();

        Assert.assertEquals(Arrays.asList("hello","world"), Cache.getInstance().get());
    }

    @Test
    public void mongoTest(){
        putEventInChannel("{\"menu\":{\"id\":\"file\",\"value\":\"File\",\"popup\":{\"menuitem\":[{\"value\":\"New\",\"onclick\":\"CreateNewDoc()\"},{\"value\":\"Open\",\"onclick\":\"OpenDoc()\"},{\"value\":\"Close\",\"onclick\":\"CloseDoc()\"}]}}}");
        putEventInChannel("{\"menu\":{\"id\":\"file2\",\"value\":\"File\",\"popup\":{\"menuitem\":[{\"value\":\"New\",\"onclick\":\"CreateNewDoc()\"},{\"value\":\"Open\",\"onclick\":\"OpenDoc()\"},{\"value\":\"Close\",\"onclick\":\"CloseDoc()\"}]}}}");
        mongoSink.process();
        mongoSink.process();

        List<Document> listResults = MongoConnector.getInstance().getAll();
        Assert.assertEquals(listResults.size(), 2);

    }

    private static void putEventInChannel(String data){
        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody(data.getBytes());
        channel.put(event);
        tx.commit();
        tx.close();
    }

}
