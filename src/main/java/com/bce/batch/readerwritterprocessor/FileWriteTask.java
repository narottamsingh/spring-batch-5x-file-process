package com.bce.batch.readerwritterprocessor;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bce.batch.dto.Customer;

class FileWriteTask implements Runnable {
    private static final Lock lock = new ReentrantLock();
    private final String threadId;
    private final List<Customer> data;

    public FileWriteTask(String threadId, List<Customer> items) {
        this.threadId = threadId;
        this.data = items;
    }

    @Override
    public void run() {
        lock.lock();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt", true))) {
            writer.write("Thread " + threadId + ": " + data.toString());
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
