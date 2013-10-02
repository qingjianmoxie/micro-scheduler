package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Node {

    private Runnable runnable;
    private ExecutorService executor;
    private List<Node> predecessors = new ArrayList<Node>();
    private Future<?> future;

    public Node(Runnable runnable, ExecutorService executor, Node... predecessors) {
        this.runnable = runnable;
        this.executor = executor;
        for (Node predecessor : predecessors) {
            this.predecessors.add(predecessor);
        }
    }

    public void tryStart() {

        if (future != null) {
            return;
        }

        boolean isReady = true;
        for (Node predecessor : predecessors) {
            if (!predecessor.isDone()) {
                isReady = false;
                break;
            }
        }
        if (!isReady) {
            return;
        }

        future = executor.submit(runnable);
    }

    public boolean isDone() {
        return future != null && future.isDone();
    }

}
