package com.pablocano.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JsonInterceptorTest {

    private static JsonInterceptor interceptor;

    @BeforeClass
    public static void beforeClass(){

        JsonInterceptor.Builder builder = new JsonInterceptor.Builder();

        Context context = new Context();
        context.put("discard-key", "discard");
        builder.configure(context);
        interceptor = (JsonInterceptor) builder.build();
    }

    @AfterClass
    public static void afterClass(){
        interceptor.close();
    }

    @Test
    public void test() {
        List<Event> listEvents = new ArrayList<>();
        listEvents.add(buildEventTest("event no valid"));
        listEvents.add(buildEventTest("{\"menu\":{\"id\":\"file\",\"value\":\"File\",\"popup\":{\"menuitem\":[{\"value\":\"New\",\"onclick\":\"CreateNewDoc()\"},{\"value\":\"Open\",\"onclick\":\"OpenDoc()\"},{\"value\":\"Close\",\"onclick\":\"CloseDoc()\"}]}}}"));

        List<Event> listEventsIntercepted = interceptor.intercept(listEvents);

        System.out.println(listEventsIntercepted);

        Assert.assertEquals(listEventsIntercepted.get(0).getBody().length, 0);
        Assert.assertEquals(listEventsIntercepted.get(0).getHeaders().get("destination"), "discard");

        Assert.assertNotNull(listEventsIntercepted.get(1).getBody());
        Assert.assertEquals(listEventsIntercepted.get(1).getBody().length, 183);
        Assert.assertEquals(new String(listEventsIntercepted.get(1).getBody()), "{\"menu\":{\"id\":\"file\",\"value\":\"File\",\"popup\":{\"menuitem\":[{\"value\":\"New\",\"onclick\":\"CreateNewDoc()\"},{\"value\":\"Open\",\"onclick\":\"OpenDoc()\"},{\"value\":\"Close\",\"onclick\":\"CloseDoc()\"}]}}}");
        Assert.assertTrue(listEventsIntercepted.get(1).getHeaders().isEmpty());
    }

    private static Event buildEventTest(String body){
        return EventBuilder.withBody(body.getBytes());
    }
}
