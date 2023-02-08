/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.stocktrades.writer;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * Generates random stock trades by picking randomly from a collection of stocks, assigning a
 * random price based on the mean, and picking a random quantity for the shares.
 *
 */

public class WavGenerator implements Runnable {

    private static final String[] FILES = {"wav/audio-1.wav", "wav/audio-1-02.wav", "wav/audio-1-03.wav", "wav/audio-3.wav", "wav/audio-3-02.wav"};
    private volatile Thread thread;
    private volatile boolean running = true;
    private KinesisAsyncClient kinesisClient;
    private String streamName;

    public void start(KinesisAsyncClient kinesisClient, String streamName) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {

        while (running) {
            sendFile();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void sendFile() {
        String callId = UUID.randomUUID().toString();
        String fileName = getRandomName();
        System.out.println("New callId " + callId + " " + fileName);
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
            byte[] buffer = new byte[320];
            try {
                while (true) {
                    long start = System.currentTimeMillis();
                    int read = is.read(buffer);
                    if (read == -1) {
                        WavWriter.send(callId, new byte[0], kinesisClient, streamName);
                        System.out.println("End callId " + callId);
                        return;
                    } else
                    if (read == 0) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        if (read != 320)
                            System.out.println("-- to less data " + callId + " " + read);
                        WavWriter.send(callId, buffer, kinesisClient, streamName);
                    }
                    long delta = 20 - (System.currentTimeMillis() - start);
                    if (delta > 0)
                        try {
                            Thread.sleep(delta);
                        } catch (InterruptedException e) {
                        }
                    else
                        System.out.println("-- oups " + callId + " " + delta);
                }
            } catch (IOException e) {
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getRandomName() {
        return FILES[ new Double(Math.random() * (double)FILES.length).intValue() ];
    }

}
