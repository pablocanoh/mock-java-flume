package com.pablocano.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonInterceptor implements Interceptor {

    private String discardKey;

    private JsonInterceptor(String discardKey) {
        this.discardKey = discardKey;
    }

    public void initialize() {}

    public Event intercept(Event event) {
        if (event == null || event.getBody() == null || !isValidJson(event))
            event = discardEvent();

        return event;
    }

    public List<Event> intercept(List<Event> list) {
        return list
                .stream()
                .map(this::intercept)
                .collect(Collectors.toList());
    }

    public void close() {}

    private  Event discardEvent(){
        Map<String, String> headers = new HashMap<>();
        headers.put("destination", discardKey);

        return EventBuilder.withBody(null, headers);
    }

    private boolean isValidJson(Event event){
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.readTree(new String(event.getBody()));
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    public static class Builder implements Interceptor.Builder {

        private String discardKey;

        @Override
        public void configure(Context context) {
            discardKey = context.getString("discard-key");
        }

        @Override
        public Interceptor build() {
            return new JsonInterceptor(discardKey);
        }
    }


}


