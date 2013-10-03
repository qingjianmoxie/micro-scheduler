package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Topology {

    private ExecutorService executor = Executors.newCachedThreadPool();
    private Map<String, Node> nodeMap = new HashMap<String, Node>();
    private Future<Void> future;

    public void addNode(String name, Callable<Void> callable, String... predecessorNames) {

        if (nodeMap.containsKey(name)) {
            throw new IllegalArgumentException("Node name exsits.");
        }

        List<Node> predecessors = new ArrayList<Node>();
        for (String predecessorName : predecessorNames) {
            Node predecessor = nodeMap.get(predecessorName);
            if (predecessor == null) {
                throw new IllegalArgumentException("Predecessor node doesn't exist.");
            }
            predecessors.add(predecessor);
        }

        nodeMap.put(name, new Node(callable, executor, predecessors.toArray(new Node[0])));
    }

    public void start() {

        if (future != null) {
            return;
        }

        future = executor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {

                while (!Thread.interrupted()) {

                    boolean isDone = true;
                    for (Node node : nodeMap.values()) {
                        if (!node.isDone()) {
                            isDone = false;
                            node.tryStart();
                        }
                    }

                    if (isDone) {
                        break;
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {}
                }

                return null;
            }

        });
    }

    public void waitForCompletion() throws Exception {
        future.get();
        executor.shutdown();
    }

}
