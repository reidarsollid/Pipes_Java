package com.sollidsoft.pipes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.channels.Pipe.SourceChannel;

public class Master {
    public Master() {
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(processors);
        try (Selector selector = Selector.open()) {

            executeWorkers(processors, executorService, selector);

            selectorHandler(selector);

            executorService.shutdownNow();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void selectorHandler(Selector selector) throws IOException {
        while (!Thread.currentThread().isInterrupted()) {
            int readyKeys = selector.select();
            if (readyKeys == 0) continue;
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            for (SelectionKey key : selectionKeys) {
                if (key.isReadable()) {
                    ReadableByteChannel readableByteChannel = (ReadableByteChannel) key.channel();
                    System.out.println("Bytes read " + handleEvent(readableByteChannel));
                    key.cancel();
                }
            }
        }
    }

    private void executeWorkers(int processors, ExecutorService executorService, Selector selector) throws IOException {
        for (int i = 0; i < processors; i++) {
            Pipe pipe = Pipe.open();
            SourceChannel sourceChannel = pipe.source();
            sourceChannel.configureBlocking(false);
            sourceChannel.register(selector, SelectionKey.OP_READ);
            executorService.execute(new Worker(pipe.sink()));
        }
    }

    private int handleEvent(ReadableByteChannel readableByteChannel) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.clear();
        int retval = readableByteChannel.read(byteBuffer);
        byteBuffer.rewind();
        long result = byteBuffer.getLong();
        System.out.println("Result : " + result);
        return retval;
    }

    public static void main(String[] args) {
        new Master();
    }

}
