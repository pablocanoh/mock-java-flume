package com.pablocano.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public abstract class CustomSink extends AbstractSink implements Configurable {

    @Override
    public void configure(Context context) {}

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop () {
        super.stop();
    }

    public boolean processEvent(Event event){
        return true;
    }

    @Override
    public Status process() {
        Status status;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do

            Event event = ch.take();
            if (!processEvent(event)){
                throw new Exception("returned false when processing the event"+event);
            }

            // Send the Event to the external repository.
            // storeSomeData(e);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }
        return status;
    }
}
