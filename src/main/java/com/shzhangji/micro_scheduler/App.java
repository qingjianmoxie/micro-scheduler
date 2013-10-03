package com.shzhangji.micro_scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class App {

    public static void main(String[] args) throws Exception {
        methodA();
        methodB();
        methodC();
    }

    /**
     * Manual scheduling
     */
    public static void methodA() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        Future<Void> futureA = executor.submit(new SqlTask("A"));
        Future<Void> futureC = executor.submit(new SqlTask("C"));
        futureA.get();
        Future<Void> futureB = executor.submit(new SqlTask("B"));
        futureB.get();
        futureC.get();
        Future<Void> futureD = executor.submit(new SqlTask("D"));
        futureD.get();
        executor.shutdown();
    }

    /**
     * Iterate all nodes every 100ms
     */
    public static void methodB() throws Exception {
        Topology topology = new Topology();
        topology.addNode("A", new SqlTask("A"));
        topology.addNode("B", new SqlTask("B"), "A");
        topology.addNode("C", new SqlTask("C"));
        topology.addNode("D", new SqlTask("D"), "B", "C");
        topology.start();
        topology.waitForCompletion();
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
        container.shutdown();
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
