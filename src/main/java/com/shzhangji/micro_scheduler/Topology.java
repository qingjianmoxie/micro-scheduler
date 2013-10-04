package com.shzhangji.micro_scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Topology<T> {

    private ExecutorService executor = Executors.newCachedThreadPool();
    private Map<String, Node<T>> nodeMap = new HashMap<String, Node<T>>();

    public void addNode(String name, Callable<T> callable, String... predecessorNames) {

        if (nodeMap.containsKey(name)) {
            throw new IllegalArgumentException("Node name exsits.");
        }

        List<Node<T>> predecessors = new ArrayList<Node<T>>();
        for (String predecessorName : predecessorNames) {
            Node<T> predecessor = nodeMap.get(predecessorName);
            if (predecessor == null) {
                throw new IllegalArgumentException("Predecessor node doesn't exist.");
            }
            predecessors.add(predecessor);
        }

        nodeMap.put(name, new Node<T>(callable, executor, predecessors));
    }

    public void run() throws Exception {

        try {

            while (!Thread.interrupted()) {

                boolean isDone = true;
                for (Node<T> node : nodeMap.values()) {
                    if (!node.isDone()) {
                        isDone = false;
                        node.tryStart();
                    }
                }

                if (isDone) {
                    break;
                }

                Thread.sleep(100);
            }

        } finally {
            executor.shutdown();
        }
    }

}
