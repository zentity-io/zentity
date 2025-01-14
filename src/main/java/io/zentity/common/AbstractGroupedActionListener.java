/*
 * zentity
 * Copyright © 2018-2025 Dave Moore
 * https://zentity.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zentity.common;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
