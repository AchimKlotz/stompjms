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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Promise<T> implements Future<T>, Callback<T> {
    private static enum State {
        WAITING,
        DONE,
        RECEIVED,
        CANCELLED
    }

    private volatile State state = State.WAITING;
    private final CountDownLatch latch = new CountDownLatch(1);
    private T result = null;

    Throwable error;

    @Override
    public void onFailure(Throwable value) {
        if (state == State.CANCELLED) {
            return;
        }
        error = value;
        state = State.DONE;
        latch.countDown();
    }

    @Override
    public void onSuccess(T value) {
        if (state == State.CANCELLED) {
            return;
        }
        state = State.DONE;
        result = value;
        latch.countDown();
    }

    @Override
    public T get(long amount, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        if (state == State.RECEIVED) {
            return result;
        }
        mayThrowFutureException();
        latch.await(amount, unit);
        if (isDone()) {
            return getInternal();
        }

        throw new TimeoutException();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        if (state == State.CANCELLED) {
            return result;
        }
        mayThrowFutureException();
        latch.await();
        return getInternal();
    }

    private T getInternal() throws ExecutionException, Error {
        if (state == State.CANCELLED) {
            return result;
        }
        mayThrowFutureException();
        state = State.RECEIVED;
        return result;
    }

    private void mayThrowFutureException() throws Error, ExecutionException {
        Throwable e = error;
        if (e instanceof Exception) {
            throw new ExecutionException(e);
        }
        else if (e instanceof Error) {
            throw (Error) e;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (state == State.DONE || state == State.RECEIVED) {
            return false;
        }
        state = State.CANCELLED;
        if (mayInterruptIfRunning) {
            error = new InterruptedException("Future has been cancelled");
            latch.countDown();
        }
        return true;
    }

    @Override
    public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public boolean isDone() {
        return state == State.DONE || state == State.RECEIVED || state == State.CANCELLED;
    }
}
