package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Node<T> {

    private Callable<T> callable;
    private ExecutorService executor;
    private List<Node<T>> predecessors = new ArrayList<Node<T>>();
    private Future<T> future;

    public Node(Callable<T> callable, ExecutorService executor, List<Node<T>> predecessors) {
        this.callable = callable;
        this.executor = executor;
        for (Node<T> predecessor : predecessors) {
            this.predecessors.add(predecessor);
        }
    }

    public void tryStart() throws Exception {

        if (future != null) {
            return;
        }

        boolean isReady = true;
        for (Node<T> predecessor : predecessors) {
            if (!predecessor.isDone()) {
                isReady = false;
                break;
            }
            predecessor.get();
        }
        if (!isReady) {
            return;
        }

        future = executor.submit(callable);
    }

    public boolean isDone() {
        return future != null && future.isDone();
    }

    public T get() throws Exception {
        return future.get();
    }

}
