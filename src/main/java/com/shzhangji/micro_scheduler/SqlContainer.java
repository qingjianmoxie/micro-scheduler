package com.shzhangji.micro_scheduler;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.Future;

public class SqlContainer extends Container<Void> {

    private static final String msPrefix = "-- ms: ";

    public Future<Void> addSql(InputStream in) throws Exception {

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sql = new StringBuilder();
        String line, prefix = null;
        Future<Void> future = null;
        while ((line = reader.readLine()) != null) {

            if (line.startsWith(msPrefix)) {

                if (prefix != null && sql.length() > 0) {
                    future = addSqlInternal(prefix, sql.toString());
                }

                prefix = line;
                sql = new StringBuilder();

            } else {
                sql.append(line).append("\n");
            }

        }

        if (prefix != null && sql.length() > 0) {
            future = addSqlInternal(prefix, sql.toString());
        }

        return future;
    }

    private Future<Void> addSqlInternal(String prefix, String sql) throws Exception{

        String[] segs = prefix.substring(7).split(",");
        String name = segs[0].trim();

        String[] predecessors;
        if (segs.length > 1) {

            predecessors = Arrays.copyOfRange(segs, 1, segs.length);
            for (int i = 0; i < predecessors.length; ++i) {
                predecessors[i] = predecessors[i].trim();
            }
            predecessors[0] = predecessors[0].substring(1);
            predecessors[predecessors.length - 1] =
                    predecessors[predecessors.length - 1].substring(
                            0, predecessors[predecessors.length - 1].length() - 1);

        } else {
            predecessors = new String[0];
        }

        return addTask(name, new SqlTask(sql), predecessors);
    }

}
