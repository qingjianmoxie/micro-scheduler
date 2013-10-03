package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Node {

    private Callable<Void> callable;
    private ExecutorService executor;
    private List<Node> predecessors = new ArrayList<Node>();
    private Future<Void> future;

    public Node(Callable<Void> callable, ExecutorService executor, Node... predecessors) {
        this.callable = callable;
        this.executor = executor;
        for (Node predecessor : predecessors) {
            this.predecessors.add(predecessor);
        }
    }

    public void tryStart() throws Exception {

        if (future != null) {
            return;
        }

        boolean isReady = true;
        for (Node predecessor : predecessors) {
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

    public Void get() throws Exception {
        return future.get();
    }

}
