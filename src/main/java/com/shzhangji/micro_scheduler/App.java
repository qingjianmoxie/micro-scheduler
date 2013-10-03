package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class App {

    public static void main(String[] args) throws Exception {

        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

        ListenableFuture<Void> futureA = executor.submit(new SqlTask("A"));
        ListenableFuture<Void> futureB = Futures.transform(futureA, new AsyncFunction<Void, Void>() {

            @Override
            public ListenableFuture<Void> apply(Void input) throws Exception {
                return executor.submit(new SqlTask("B"));
            }

        }, executor);
        ListenableFuture<Void> futureC = executor.submit(new SqlTask("C"));

        List<ListenableFuture<Void>> futureBC = new ArrayList<ListenableFuture<Void>>();
        futureBC.add(futureB);
        futureBC.add(futureC);
        ListenableFuture<Void> futureD = Futures.transform(Futures.allAsList(futureBC), new AsyncFunction<List<Void>, Void>() {

            @Override
            public ListenableFuture<Void> apply(List<Void> input)
                    throws Exception {
                return executor.submit(new SqlTask("D"));
            }

        }, executor);
        futureD.get();
        executor.shutdown();

    }

    public static class SqlTask implements Callable<Void> {

        private String sql;

        public SqlTask(String sql) {
            this.sql = sql;
        }

        @Override
        public Void call() throws Exception {
            System.out.println("Processing sql \"" + sql + "\"...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {}
            return null;
        }

    }
}
