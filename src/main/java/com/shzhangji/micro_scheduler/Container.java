package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class Container<T> {

    private ListeningExecutorService executor = MoreExecutors.listeningDecorator(
            MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));
    private Map<String, ListenableFuture<T>> futureMap = new HashMap<String, ListenableFuture<T>>();

    public Future<T> addTask(String name, final Callable<T> callable, String... predecessorNames) {

        if (futureMap.containsKey(name)) {
            throw new IllegalArgumentException("Task name exists.");
        }

        List<ListenableFuture<T>> predecessorFutures = new ArrayList<ListenableFuture<T>>();
        for (String predecessorName : predecessorNames) {
            ListenableFuture<T> predecessorFuture = futureMap.get(predecessorName);
            if (predecessorFuture == null) {
                throw new IllegalArgumentException("Predecessor task doesn't exist.");
            }
            predecessorFutures.add(predecessorFuture);
        }

        ListenableFuture<T> future;
        if (predecessorFutures.isEmpty()) {
            future = executor.submit(callable);
        } else {
            future = Futures.transform(Futures.allAsList(predecessorFutures), new AsyncFunction<List<T>, T>() {

                @Override
                public ListenableFuture<T> apply(List<T> input) throws Exception {
                    return executor.submit(callable);
                }

            }, executor);
        }
        futureMap.put(name, future);
        return future;
    }

}
