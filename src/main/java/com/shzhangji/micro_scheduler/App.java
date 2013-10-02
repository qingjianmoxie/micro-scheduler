package com.shzhangji.micro_scheduler;

public class App {

    public static void main(String[] args) throws Exception {

        Topology topology = new Topology();
        topology.addNode("A", new SqlTask("A"));
        topology.addNode("B", new SqlTask("B"), "A");
        topology.addNode("C", new SqlTask("C"));
        topology.addNode("D", new SqlTask("D"), "B", "C");
        topology.start();
        topology.waitForCompletion();

    }

    public static class SqlTask implements Runnable {

        private String sql;

        public SqlTask(String sql) {
            this.sql = sql;
        }

        @Override
        public void run() {
            System.out.println("Processing sql \"" + sql + "\"...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {}
        }

    }
}
