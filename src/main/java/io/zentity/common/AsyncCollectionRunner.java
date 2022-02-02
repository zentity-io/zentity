/*
 * zentity
 * Copyright © 2018-2022 Dave Moore
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

import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

/**
 * A utility class that runs items in a collection asynchronously and collects their results in order.
 *
 * @param <T>
 * @param <ResultT>
 */
public class AsyncCollectionRunner<T, ResultT> {
    private final BiConsumer<T, ActionListener<ResultT>> itemRunner;
    private final Deque<T> items;
    private final boolean failFast;
    private final int concurrency;
    private final int size;

    // state
    private IndexedGroupedActionListener<ResultT> groupedListener;
    private boolean hasStarted = false;
    private boolean hasFailure = false;

    public AsyncCollectionRunner(Collection<T> items, BiConsumer<T, ActionListener<ResultT>> itemRunner) {
        this(items, itemRunner, 1, false);
    }

    public AsyncCollectionRunner(Collection<T> items, BiConsumer<T, ActionListener<ResultT>> itemRunner, int concurrency) {
        this(items, itemRunner, concurrency, false);
    }

    public AsyncCollectionRunner(Collection<T> items, BiConsumer<T, ActionListener<ResultT>> itemRunner, int concurrency, boolean failFast) {
        this.items = new ConcurrentLinkedDeque<>(items);
        this.itemRunner = itemRunner;
        this.size = items.size();
        this.concurrency = concurrency;
        this.failFast = failFast;
    }

    private void runNextItem() {
        if (hasFailure && groupedListener.failFast()) {
            // Don't continue running if there is already a failure
            // and the failure has already been delegated
            return;
        }

        T nextItem;
        final int resultIndex;

        synchronized (items) {
            if (items.isEmpty()) {
                return;
            }
            resultIndex = size - items.size();
            nextItem = items.pop();
        }

        ActionListener<ResultT> resultListener = ActionListener.wrap(
            (result) -> groupedListener.onResponse(resultIndex, result),
            (ex) -> {
                hasFailure = true;
                groupedListener.onFailure(ex);
            }
        );

        itemRunner.accept(nextItem, ActionListener.runAfter(
            resultListener,
            this::runNextItem));
    }

    /**
     * Run the collection and listen for the results.
     *
     * @param onComplete The result listener.
     */
    public void run(ActionListener<Collection<ResultT>> onComplete) {
        synchronized (this) {
            if (hasStarted) {
                throw new IllegalStateException("Runner has already been started. Instances cannot be reused.");
            }
            hasStarted = true;
        }

        groupedListener = new IndexedGroupedActionListener<>(onComplete, items.size(), failFast);

        IntStream.range(0, concurrency)
            .forEach((i) -> runNextItem());
    }

    static class IndexedGroupedActionListener<ResultT> extends AbstractGroupedActionListener<ResultT> {
        IndexedGroupedActionListener(ActionListener<Collection<ResultT>> delegate, int groupSize, boolean failFast) {
            super(delegate, groupSize, failFast);
        }

        void onResponse(int index, ResultT element) {
            this.setResultAndCountDown(index, element);
        }

        @Override
        public void onResponse(ResultT element) {
            throw new IllegalStateException("Should call onResponse(int index, ResultT element) instead.");
        }
    }
}
