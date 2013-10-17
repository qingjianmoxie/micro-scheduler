package com.shzhangji.micro_scheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Future;

public class SqlContainer extends Container<Void> {

    public Future<Void> addSql(InputStream in) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        try {
            System.out.println(reader.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
