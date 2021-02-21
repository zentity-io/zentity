package io.zentity.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class AsyncCollectionRunnerTest {
  static final Executor THREAD_PER_TASK_EXECUTOR = (command) -> new Thread(command).start();

  void quietSleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
    }
  }

  @Test
  public void testRunSerial() throws InterruptedException, ExecutionException {
    List<Integer> items = List.of(0, 1, 2, 3, 4);

    BiConsumer<Integer, ActionListener<Integer>> itemRunner = (num, listener) -> THREAD_PER_TASK_EXECUTOR.execute(() -> {
      quietSleep(1);
      listener.onResponse(num);
    });

    CompletableFuture<Collection<Integer>> doneFut = new CompletableFuture<>();

    AsyncCollectionRunner<Integer, Integer> runner = new AsyncCollectionRunner<>(
        items,
        itemRunner);

    runner.run(ActionListener.wrap(doneFut::complete, doneFut::completeExceptionally));

    Collection<Integer> results = doneFut.get();
    assertEquals(items, results);
  }

  @Test
  public void testRunParallel() throws InterruptedException, ExecutionException {
    int size = 1_000;
    List<Integer> items = IntStream.range(0, size)
        .boxed()
        .collect(Collectors.toList());

    BiConsumer<Integer, ActionListener<Integer>> itemRunner = (num, listener) -> THREAD_PER_TASK_EXECUTOR.execute(() -> {
      quietSleep(1);
      listener.onResponse(num);
    });

    CompletableFuture<Collection<Integer>> doneFut = new CompletableFuture<>();

    AsyncCollectionRunner<Integer, Integer> runner = new AsyncCollectionRunner<>(
        items,
        itemRunner,
        50);

    runner.run(
        ActionListener.wrap(doneFut::complete, doneFut::completeExceptionally));

    Collection<Integer> results = doneFut.get();
    assertEquals(items, results);
  }

  @Test
  public void testRunHigherConcurrencyThanItems() throws InterruptedException, ExecutionException {
    int size = 4;
    List<Integer> items = IntStream.range(0, size)
        .boxed()
        .collect(Collectors.toList());

    BiConsumer<Integer, ActionListener<Integer>> itemRunner = (num, listener) -> THREAD_PER_TASK_EXECUTOR.execute(() -> {
      quietSleep(1);
      listener.onResponse(num);
    });

    CompletableFuture<Collection<Integer>> doneFut = new CompletableFuture<>();

    AsyncCollectionRunner<Integer, Integer> runner = new AsyncCollectionRunner<>(
        items,
        itemRunner,
        50);

    runner.run(
        ActionListener.wrap(doneFut::complete, doneFut::completeExceptionally));

    Collection<Integer> results = doneFut.get();
    assertEquals(items, results);
  }
}
