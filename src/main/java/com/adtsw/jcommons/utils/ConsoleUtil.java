package com.adtsw.jcommons.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConsoleUtil {

    public static String getInput(String message) {

        System.out.println(message);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
