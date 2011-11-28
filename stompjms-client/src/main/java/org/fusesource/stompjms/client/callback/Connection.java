/**
 * Copyright (C) 2010-2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * The software in this package is published under the terms of the
 * CDDL license a copy of which has been included with this distribution
 * in the license.txt file.
 */
package org.fusesource.stompjms.client.callback;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.stompjms.client.ProtocolException;
import org.fusesource.stompjms.client.StompFrame;
import org.fusesource.stompjms.client.StompProtocolCodec;
import org.fusesource.stompjms.client.transport.Transport;
import org.fusesource.stompjms.client.transport.TransportListener;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.fusesource.stompjms.client.Constants.*;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Connection {

    private static class OverflowEntry {
        final StompFrame frame;
        final Callback<Void> cb;

        OverflowEntry(StompFrame frame, Callback<Void> cb) {
            this.cb = cb;
            this.frame = frame;
        }
    }


    private final Transport transport;
    private final StompFrame connectedFrame;
    private Callback<StompFrame> receiver;
    private Runnable refiller;
    private final AtomicLong requestCounter = new AtomicLong();
    private HashMap<AsciiBuffer, Callback<StompFrame>> requests = new HashMap<AsciiBuffer, Callback<StompFrame>>();
    private LinkedList<OverflowEntry> overflow = new LinkedList<OverflowEntry>();

    private Throwable failure;

    Connection(Transport transport, StompFrame connectedFrame) {
        this.transport = transport;
        this.connectedFrame = connectedFrame;
        this.transport.setTransportListener(new TransportListener() {
            public void onTransportCommand(Object command) {
                processStompFrame((StompFrame)command);
            }
            public void onRefill() {
                drainOverflow();
            }
            public void onTransportFailure(IOException error) {
                processFailure(error);
            }
            public void onTransportConnected() {
            }
            public void onTransportDisconnected() {
            }
        });
    }

    public StompFrame connectedFrame() {
        return connectedFrame;
    }

    public Transport transport() {
        return transport;
    }

    public Connection refiller(Runnable refiller) {
        getDispatchQueue().assertExecuting();
        this.refiller = refiller;
        return this;
    }

    public Connection receive(Callback<StompFrame> receiver) {
        getDispatchQueue().assertExecuting();
        this.receiver = receiver;
        return this;
    }

    private void processStompFrame(StompFrame frame) {
        getDispatchQueue().assertExecuting();
        AsciiBuffer action = frame.action();
        if (action.equals(RECEIPT)) {
            AsciiBuffer id = frame.getHeader(RECEIPT_ID);
            if (id != null) {
                Callback<StompFrame> cb = this.requests.remove(id);
                if (cb != null) {
                    cb.success(frame);
                } else {
                    if( !toReceiver(frame) ) {
                        processFailure(new ProtocolException("Stomp Response without a valid receipt id: " + id + " for frame " + frame));
                    }
                }
            } else {
                processFailure(new ProtocolException("Stomp Response with no receipt id: " + frame));
            }
        } else if (action.startsWith(ERROR)) {
            processFailure(new ProtocolException("Received an error: " + frame.errorMessage()));
        } else {
            toReceiver(frame);
        }
    }

    private boolean toReceiver(StompFrame frame) {
        if( receiver!=null ) {
            try {
                receiver.success(frame);
            } catch (Exception e) {
                processFailure(e);
            }
            return true;
        }
        return false;
    }

    private void processFailure(Throwable error) {
        if( failure == null ) {
            failure = error;
            failRequests(failure);
            if( receiver!=null ) {
                try {
                    receiver.failure(failure);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void failRequests(Throwable failure) {
        ArrayList<Callback<Void>> values = new ArrayList(requests.values());
        requests.clear();
        for (Callback<Void> value : values) {
            value.failure(failure);
        }

        ArrayList<OverflowEntry> overflowEntries = new ArrayList<OverflowEntry>(overflow);
        overflow.clear();
        for (OverflowEntry entry : overflowEntries) {
            if( entry.cb !=null ) {
                entry.cb.failure(failure);
            }
        }
    }

    public DispatchQueue getDispatchQueue() {
        return this.transport.getDispatchQueue();
    }

    public void resume() {
        this.transport.resumeRead();
    }
    public void suspend() {
        this.transport.suspendRead();
    }

    public void close(final Runnable onComplete) {
        this.transport.stop(new Runnable() {
            public void run() {
                if( onComplete!=null ) {
                    failRequests(new ClosedChannelException());
                    onComplete.run();
                }
            }
        });
    }

    public boolean offer(StompFrame frame) {
        return this.offer(frame, true);
    }

    public boolean offer(StompFrame frame, boolean addContentLength) {
        getDispatchQueue().assertExecuting();
        if( this.transport.full() ) {
            return false;
        } else {
            if( addContentLength && SEND.equals(frame.action()) ) {
                frame.addContentLengthHeader();
            }
            return this.transport.offer(frame);
        }
    }

    public boolean full() {
        getDispatchQueue().assertExecuting();
        return this.transport.full();
    }

    public Throwable getFailure() {
        getDispatchQueue().assertExecuting();
        return failure;
    }

    public AsciiBuffer nextId() {
        return new AsciiBuffer(Long.toString(requestCounter.incrementAndGet()));
    }
    public AsciiBuffer nextId(String prefix) {
        return new AsciiBuffer(prefix+(requestCounter.incrementAndGet()));
    }

    public void request(StompFrame frame, Callback<StompFrame> cb) {
        getDispatchQueue().assertExecuting();
        assert cb!=null : "Callback must not be null";
        if( failure !=null ) {
            cb.failure(failure);
        } else {
            AsciiBuffer id = nextId();
            this.requests.put(id, cb);
            frame.addHeader(RECEIPT_REQUESTED, id);
            send(frame, null);
        }
    }

    private void drainOverflow() {
        getDispatchQueue().assertExecuting();
        if( overflow.isEmpty() ){
            return;
        }
        OverflowEntry entry;
        while((entry=overflow.peek())!=null) {
            if( offer(entry.frame) ) {
                overflow.removeFirst();
                if( entry.cb!=null ) {
                    entry.cb.success(null);
                }
            } else {
                break;
            }
        }
        if( overflow.isEmpty() ) {
            if( refiller!=null ) {
                try {
                    refiller.run();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void send(StompFrame frame, Callback<Void> cb) {
        getDispatchQueue().assertExecuting();
        if( failure !=null ) {
            if( cb!=null ) {
                cb.failure(failure);
            }
        } else {
            if( overflow.isEmpty() && offer(frame) ) {
                if( cb!=null ) {
                    cb.success(null);
                }
            } else {
                overflow.addLast(new OverflowEntry(frame, cb));
            }
        }
    }


}
