package com.pablocano.sink;

import com.pablocano.persistance.db.Cache;
import com.pablocano.util.MongoEmbeded;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class PersintanceSinkTest {

    private static CustomSink sink;
    private static Channel channel;

    @BeforeClass
    public static void beforeClass() throws IOException {

        new MongoEmbeded();

        channel = new MemoryChannel();
        Context channelContext = new Context();
        Configurables.configure(channel, channelContext);

        sink = new PersintanceSink();
        Context sinkContext = new Context();
        sinkContext.put("persistence-class-name", "com.pablocano.persistance.MemoryPersistance");
        Configurables.configure(sink, sinkContext);
        sink.setChannel(channel);

        channel.start();
        sink.start();
    }

    @AfterClass
    public static void afterClass(){
        sink.stop();
        channel.stop();
    }

    @Test
    public void test(){
        putEventInChannel("hello");
        putEventInChannel("world");
        sink.process();
        sink.process();

        Assert.assertEquals(Arrays.asList("hello","world"), Cache.getInstance().get());
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
