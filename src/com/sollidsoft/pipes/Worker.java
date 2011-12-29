package com.sollidsoft.pipes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Worker implements Runnable {
    private final WritableByteChannel writableByteChannel;

    public Worker(WritableByteChannel writableByteChannel) {
        this.writableByteChannel = writableByteChannel;
    }


    public void run() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8);
        byteBuffer.clear();
        long threadId = Thread.currentThread().getId();
        byteBuffer.putLong(threadId);
        byteBuffer.flip();
        try {
            int sleepInt = new Random().nextInt(5);
            TimeUnit.SECONDS.sleep(sleepInt);
            System.out.println("Thread id " + threadId + " doing some work for " + sleepInt + " seconds");
            writableByteChannel.write(byteBuffer);
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

    }

}
