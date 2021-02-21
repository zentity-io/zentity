package io.zentity.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;

import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

/**
 * A grouped {@link ActionListener} that runs a collection one after another.
 * @param <T>
 * @param <ResultT>
 */
public class CollectionRunnerActionListener<T, ResultT> extends ErrorSuppressingGroupedActionListener<ResultT> {
    BiConsumer<T, ActionListener<ResultT>> itemRunner;
    Deque<T> items;

    public CollectionRunnerActionListener(ActionListener<Tuple<Collection<ResultT>, Exception>> delegate, Collection<T> items, BiConsumer<T, ActionListener<ResultT>> itemRunner) {
        super(delegate, items.size());
        this.items = new ConcurrentLinkedDeque<>(items);
        this.itemRunner = itemRunner;
    }

    private void runNextItem() {
        if (items.isEmpty()) {
            return;
        }

        T nextItem = items.pop();

        itemRunner.accept(nextItem, ActionListener.runAfter(this, this::runNextItem));
    }

    public void start() {
        start(1);
    }

    public void start(int concurrency) {
        // TODO: need to order results for this to work
        IntStream.range(0, concurrency)
                .forEach((i) -> runNextItem());
    }
}
