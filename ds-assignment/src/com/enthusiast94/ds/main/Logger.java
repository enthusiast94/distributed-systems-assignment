package com.enthusiast94.ds.main;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by manas on 05-11-2015.
 */
public class Logger {

    private static final String TAG = Logger.class.getSimpleName();
    private String fileName;
    private static Logger instance;
    private StringBuilder stringBuilder;

    private Logger(String fileName) {
        this.fileName = fileName;
        stringBuilder = new StringBuilder();
    }

    public static void init(String fileName) {
        instance = new Logger(fileName);
    }

    public static Logger getInstance() {
        if (instance == null) {
            throw new IllegalStateException(TAG + " has not been initialized yet");
        }

        return instance;
    }

    public void addMessage(String message) {
        stringBuilder.append(message).append("\n");
    }

    public void logToFile() throws IOException {
        FileOutputStream fos = new FileOutputStream(fileName);
        fos.write(stringBuilder.toString().getBytes());
        fos.close();

        stringBuilder = new StringBuilder();
    }
}
