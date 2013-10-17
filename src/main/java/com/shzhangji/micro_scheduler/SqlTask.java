package com.shzhangji.micro_scheduler;

import java.util.concurrent.Callable;

public class SqlTask implements Callable<Void> {

    private String sql;

    public SqlTask(String sql) {
        this.sql = sql;
    }

    @Override
    public Void call() throws Exception {
        System.out.println("Processing sql \"" + sql + "\"...");
//        if (sql.equals("B")) {
//            throw new Exception("Fail to process sql B.");
//        }
        Thread.sleep(2000);
        return null;
    }

}
