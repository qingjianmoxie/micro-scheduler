package com.shzhangji.micro_scheduler;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class App {

    public static void main(String[] args) throws Exception {
//        methodA();
//        methodB();
//        methodC();
//        methodD();
        methodE();
    }

    /**
     * Manual scheduling
     */
    public static void methodA() throws Exception {

        ExecutorService executor = Executors.newCachedThreadPool();
        try {

            Future<Void> futureA = executor.submit(new SqlTask("A"));
            Future<Void> futureC = executor.submit(new SqlTask("C"));
            futureA.get();
            Future<Void> futureB = executor.submit(new SqlTask("B"));
            futureB.get();
            futureC.get();
            Future<Void> futureD = executor.submit(new SqlTask("D"));
            futureD.get();

        } finally {
            executor.shutdown();
        }
    }

    /**
     * Iterate all nodes every 100ms
     */
    public static void methodB() throws Exception {
        Topology<Void> topology = new Topology<Void>();
        topology.addNode("A", new SqlTask("A"));
        topology.addNode("B", new SqlTask("B"), "A");
        topology.addNode("C", new SqlTask("C"));
        topology.addNode("D", new SqlTask("D"), "B", "C");
        topology.run();
    }

    /**
     * Use Guava's ListenableFuture
     */
    public static void methodC() throws Exception {
        Container<Void> container = new Container<Void>();
        container.addTask("A", new SqlTask("A"));
        container.addTask("B", new SqlTask("B"), "A");
        container.addTask("C", new SqlTask("C"));
        container.addTask("D", new SqlTask("D"), "B", "C").get();
    }

    @SuppressWarnings("unchecked")
    public static void methodD() throws Exception {

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(
                MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));
        ListenableFuture<Void> futureA = newTask(new SqlTask("A"), executor);
        ListenableFuture<Void> futureB = newTask(new SqlTask("B"), executor, futureA);
        ListenableFuture<Void> futureC = newTask(new SqlTask("C"), executor);
        ListenableFuture<Void> futureD = newTask(new SqlTask("D"), executor, futureB, futureC);
        futureD.get();

    }

    public static void methodE() throws Exception {

        SqlContainer sqlContainer = new SqlContainer();
        sqlContainer.addSql(App.class.getResourceAsStream("/flow.sql")).get();

    }

    private static <T> ListenableFuture<T> newTask(final Callable<T> callable, final ListeningExecutorService executor,
            ListenableFuture<T>... predecessors) {

        if (predecessors.length > 0) {
            return Futures.transform(Futures.allAsList(Arrays.asList(predecessors)), new AsyncFunction<List<T>, T>() {

                @Override
                public ListenableFuture<T> apply(List<T> input)
                        throws Exception {
                    return executor.submit(callable);
                }

            }, executor);
        } else {
            return executor.submit(callable);
        }
    }

}
