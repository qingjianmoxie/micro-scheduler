package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Topology {

    private ExecutorService executor = Executors.newCachedThreadPool();
    private Map<String, Node> nodeMap = new HashMap<String, Node>();
    private Future<?> future;

    public void addNode(String name, Runnable runnable, String... predecessorNames) {

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

        nodeMap.put(name, new Node(runnable, executor, predecessors.toArray(new Node[0])));
    }

    public void start() {

        if (future != null) {
            return;
        }

        future = executor.submit(new Runnable() {

            @Override
            public void run() {

                while (!Thread.interrupted()) {

                    boolean isDone = true;
                    for (Node node : nodeMap.values()) {
                        node.tryStart();
                        if (!node.isDone()) {
                            isDone = false;
                        }
                    }

                    if (isDone) {
                        break;
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {}
                }

            }

        });
    }

    public void waitForCompletion() throws Exception {
        future.get();
        executor.shutdown();
    }

}
