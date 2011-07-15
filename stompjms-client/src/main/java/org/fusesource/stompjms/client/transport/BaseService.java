/**
 * Copyright (C) 2010-2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * The software in this package is published under the terms of the
 * CDDL license a copy of which has been included with this distribution
 * in the license.txt file.
 */
package org.fusesource.stompjms.client.transport;

import org.fusesource.hawtdispatch.DispatchQueue;

import java.util.LinkedList;

/**
 * <p>
 * The BaseService provides helpers for dealing async service state.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class BaseService {

    public static class State {
        public String toString() {
            return getClass().getSimpleName();
        }
        public boolean isStarted() {
            return false;
        }
    }

    static class CallbackSupport extends State {
        LinkedList<Runnable> callbacks = new LinkedList<Runnable>();

        void add(Runnable r) {
            if (r != null) {
                callbacks.add(r);
            }
        }

        void done() {
            for (Runnable callback : callbacks) {
                callback.run();
            }
        }
    }

    public static final State CREATED = new State();
    public static class STARTING extends CallbackSupport {
    }
    public static final State STARTED = new State() {
        public boolean isStarted() {
            return true;
        }
    };
    public static class STOPPING extends CallbackSupport {
    }

    public static final State STOPPED = new State();


    protected State _serviceState = CREATED;

    final public void start(final Runnable onCompleted) {
        getDispatchQueue().execute(new Runnable() {
            public void run() {
                if (_serviceState == CREATED ||
                        _serviceState == STOPPED) {
                    final STARTING state = new STARTING();
                    state.add(onCompleted);
                    _serviceState = state;
                    _start(new Runnable() {
                        public void run() {
                            _serviceState = STARTED;
                            state.done();
                        }
                    });
                } else if (_serviceState instanceof STARTING) {
                    ((STARTING) _serviceState).add(onCompleted);
                } else if (_serviceState == STARTED) {
                    if (onCompleted != null) {
                        onCompleted.run();
                    }
                } else {
                    if (onCompleted != null) {
                        onCompleted.run();
                    }
                    error("start should not be called from state: " + _serviceState);
                }
            }
        });
    }

    final public void stop(final Runnable onCompleted) {
        getDispatchQueue().execute(new Runnable() {
            public void run() {
                if (_serviceState == STARTED) {
                    final STOPPING state = new STOPPING();
                    state.add(onCompleted);
                    _serviceState = state;
                    _stop(new Runnable() {
                        public void run() {
                            _serviceState = STOPPED;
                            state.done();
                        }
                    });
                } else if (_serviceState instanceof STOPPING) {
                    ((STOPPING) _serviceState).add(onCompleted);
                } else if (_serviceState == STOPPED) {
                    if (onCompleted != null) {
                        onCompleted.run();
                    }
                } else {
                    if (onCompleted != null) {
                        onCompleted.run();
                    }
                    error("stop should not be called from state: " + _serviceState);
                }
            }
        });
    }

    private void error(String msg) {
        try {
            throw new AssertionError(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected State getServiceState() {
        return _serviceState;
    }

    abstract protected DispatchQueue getDispatchQueue();

    abstract protected void _start(Runnable onCompleted);

    abstract protected void _stop(Runnable onCompleted);

}