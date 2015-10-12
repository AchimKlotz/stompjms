/**
 * Copyright (C) 2010-2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * The software in this package is published under the terms of the
 * CDDL license a copy of which has been included with this distribution
 * in the license.txt file.
 */
package org.fusesource.stomp.client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.stomp.codec.StompFrame;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BlockingConnection {

    private final FutureConnection connection;

    BlockingConnection(FutureConnection connection) {
        this.connection = connection;
    }

    public void close() throws IOException {
        try {
            connection.close().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    public AsciiBuffer nextId() {
        return connection.nextId();
    }

    public AsciiBuffer nextId(String prefix) {
        return connection.nextId(prefix);
    }

    public StompFrame request(StompFrame frame) throws IOException {
        try {
            return connection.request(frame).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    public void send(StompFrame frame) throws IOException {
        try {
            connection.send(frame).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    public StompFrame receive() throws IOException {
        try {
            return connection.receive().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    public StompFrame connectedFrame() {
        return connection.connectedFrame();
    }

    public void resume() {
        connection.resume();
    }

    public void suspend() {
        connection.suspend();
    }
}
