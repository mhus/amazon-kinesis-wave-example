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


import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.util.LinkedList;

/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class WavWriter {

    private static final Log LOG = LogFactory.getLog(WavWriter.class);

    private static void checkUsage(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: " + WavWriter.class.getSimpleName()
                    + " <stream name> <region>");
            System.exit(1);
        }
    }

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName Name of stream
     */
    private static void validateStream(KinesisAsyncClient kinesisClient, String streamName) {
        try {
            DescribeStreamRequest describeStreamRequest =  DescribeStreamRequest.builder().streamName(streamName).build();
            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get();
            if(!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        }catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

    /**
     * Uses the Kinesis client to send the stock trade to the given stream.
     *
     * @param trade instance representing the stock trade
     * @param kinesisClient Amazon Kinesis client
     * @param streamName Name of stream
     */
    public static void send(String callId, byte[] bytes, KinesisAsyncClient kinesisClient,
                                       String streamName) {
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for stock trade");
            return;
        }

        // LOG.info("Putting: " + callId + " " + bytes.length + " bytes");
        PutRecordRequest putRecord = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(callId)
                .data(SdkBytes.fromByteArray(bytes)).build();

        try {
            kinesisClient.putRecord(putRecord);
        } catch (Exception ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }

    public static void main(String[] args) throws Exception {
        checkUsage(args);

        String streamName = args[0];
        String regionName = args[1];
        Region region = Region.of(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));
        System.out.println("Connected");
        // Validate that the stream exists and is active
        validateStream(kinesisClient, streamName);

        // Repeatedly send stock trades with a 100 milliseconds wait in between
        LinkedList<WavGenerator> jobs = new LinkedList<WavGenerator>();
        for (int i=0; i < 10; i++)
            jobs.add(new WavGenerator());

        jobs.forEach(j -> j.start(kinesisClient, streamName) );

    }

}
