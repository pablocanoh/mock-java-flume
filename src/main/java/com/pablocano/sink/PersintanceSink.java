package com.pablocano.sink;

import com.pablocano.persistance.Persintace;
import org.apache.flume.Context;
import org.apache.flume.Event;

public class PersintanceSink extends CustomSink {

    private Persintace persintace;

    @Override
    public void configure(Context context) {
        super.configure(context);

        try {
            persintace = (Persintace) Class.forName(context.getString("persistence-class-name")).newInstance();
            persintace.configure(context.getParameters());
            persintace.init();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean processEvent(Event event){
        persintace.save(new String(event.getBody()));
        return true;
    }
}
