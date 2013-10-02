package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class App {

    public static void main(String[] args) {

        ExecutorService executor = Executors.newCachedThreadPool();

        Task taskA = new Task("A", executor);
        Task taskB = new Task("B", executor, taskA);
        Task taskC = new Task("C", executor);
        Task taskD = new Task("D", executor, taskB, taskC);

        List<Task> tasks = new ArrayList<Task>();
        tasks.add(taskA);
        tasks.add(taskB);
        tasks.add(taskC);
        tasks.add(taskD);

        while (!Thread.interrupted()) {

            boolean isDone = true;
            for (Task task : tasks) {
                task.tryStart();
                if (!task.isDone()) {
                    isDone = false;
                }
            }

            if (isDone) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }

        executor.shutdown();

    }

    public static class Task {

        private String sql;
        private ExecutorService executor;
        private List<Task> predecessors = new ArrayList<Task>();
        private Future<?> future;

        public Task(String sql, ExecutorService executor, Task... predecessors) {
            this.sql = sql;
            this.executor = executor;
            for (Task predecessor : predecessors) {
                this.predecessors.add(predecessor);
            }
        }

        public void tryStart() {

            if (future != null) {
                return;
            }

            boolean isReady = true;
            for (Task predecessor : predecessors) {
                if (!predecessor.isDone()) {
                    isReady = false;
                    break;
                }
            }
            if (!isReady) {
                return;
            }

            future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Processing sql \"" + sql + "\"...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                    }
                }
            });
        }

        public boolean isDone() {
            return future != null && future.isDone();
        }

    }
}
