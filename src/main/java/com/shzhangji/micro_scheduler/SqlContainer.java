package com.shzhangji.micro_scheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Future;

public class SqlContainer extends Container<Void> {

    private static final String msPrefix = "-- ms: ";

    public Future<Void> addSql(InputStream in) throws Exception {

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sql = new StringBuilder();
        String line, prefix;
        while ((line = reader.readLine()) != null) {

            if (line.startsWith(msPrefix)) {

                if (sql.length() > 0) {

                }

            }

        }

        return null;
    }

}
