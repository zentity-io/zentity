package io.zentity.common;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A base {@link GroupedActionListener} that is thread-safe, allows failing quickly on error,
 * and generally is more extensible to subclasses. It guarentees that the delegate's {@link ActionListener#onResponse}
 * and {@link ActionListener#onFailure} methods will be called at-most-once and exclusively.
 *
 * @param <T>
 */
public abstract class AbstractGroupedActionListener<T> implements ActionListener<T> {
    private final CountDown countDown;
    private final AtomicArray<T> results;
    private final ActionListener<Collection<T>> delegate;
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final boolean failFast;
    private boolean responseSent = false;
    protected final int groupSize;

    public AbstractGroupedActionListener(ActionListener<Collection<T>> delegate, int groupSize) {
        this(delegate, groupSize, false);
    }

    public AbstractGroupedActionListener(ActionListener<Collection<T>> delegate, int groupSize, boolean failFast) {
        if (groupSize <= 0) {
            throw new IllegalArgumentException("groupSize must be greater than 0 but was " + groupSize);
        }
        this.groupSize = groupSize;
        this.results = new AtomicArray<>(groupSize);
        this.countDown = new CountDown(groupSize);
        this.delegate = delegate;
        this.failFast = failFast;
    }

    public boolean failFast() {
        return failFast;
    }

    private void sendResponse() {
        synchronized (this) {
            if (responseSent) {
                return;
            }
            this.responseSent = true;
        }

        if (this.failure.get() != null) {
            this.delegate.onFailure(this.failure.get());
        } else {
            List<T> collect = Collections.unmodifiableList(this.results.asList());
            this.delegate.onResponse(collect);
        }
    }

    protected void setResultAndCountDown(int index, T element) {
        this.results.setOnce(index, element);
        if (this.countDown.countDown()) {
            sendResponse();
        }
    }

    public abstract void onResponse(T element);

    public void onFailure(Exception e) {
        if (!this.failure.compareAndSet(null, e)) {
            this.failure.accumulateAndGet(e, (current, update) -> {
                if (update != current) {
                    current.addSuppressed(update);
                }

                return current;
            });
        }

        if (this.countDown.countDown() || this.failFast) {
            sendResponse();
        }
    }
}
