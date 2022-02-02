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
