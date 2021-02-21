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
 * A {@link GroupedActionListener} that buffers errors like results instead of sending failure responses.
 *
 * @param <T>
 */
public class ErrorSuppressingGroupedActionListener<T> implements ActionListener<T> {
    private final CountDown countDown;
    private final AtomicInteger pos = new AtomicInteger();
    private final AtomicArray<T> results;
    private final ActionListener<Tuple<Collection<T>, Exception>> delegate;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    public ErrorSuppressingGroupedActionListener(ActionListener<Tuple<Collection<T>, Exception>> delegate, int groupSize) {
        if (groupSize <= 0) {
            throw new IllegalArgumentException("groupSize must be greater than 0 but was " + groupSize);
        } else {
            this.results = new AtomicArray<>(groupSize);
            this.countDown = new CountDown(groupSize);
            this.delegate = delegate;
        }
    }

    private void sendResponse() {
        List<T> collect = Collections.unmodifiableList(this.results.asList());
        this.delegate.onResponse(Tuple.tuple(collect, this.failure.get()));
    }

    public void onResponse(T element) {
        this.results.setOnce(this.pos.incrementAndGet() - 1, element);
        if (this.countDown.countDown()) {
            sendResponse();
        }
    }

    public void onFailure(Exception e) {
        if (!this.failure.compareAndSet(null, e)) {
            this.failure.accumulateAndGet(e, (current, update) -> {
                if (update != current) {
                    current.addSuppressed(update);
                }

                return current;
            });
        }

        if (this.countDown.countDown()) {
            sendResponse();
        }
    }
}
