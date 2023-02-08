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

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class WavRecordProcessor implements ShardRecordProcessor {

    private static final Log log = LogFactory.getLog(WavRecordProcessor.class);

    private String kinesisShardId;
    private Map<String, FileOutputStream> fileMap;

    @Override
    public void initialize(InitializationInput initializationInput) {

        fileMap = Collections.synchronizedMap(new HashMap<String, FileOutputStream>());

        kinesisShardId = initializationInput.shardId();
        log.info("Initializing record processor for shard: " + kinesisShardId);
        log.info("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber().toString());
  }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
         try {
            processRecordsInput.records().forEach(r -> processRecord(r));
        } catch (Throwable t) {
            log.error("Caught throwable while processing records. Aborting.");
            t.printStackTrace();
            Runtime.getRuntime().halt(1);
        }

    }

    private void processRecord(KinesisClientRecord record) {

        byte[] data = SdkBytes.fromByteBuffer(record.data()).asByteArray();
        String callId = record.partitionKey();

        FileOutputStream os = fileMap.get(callId);
        if (os == null) {
            System.out.println("New callId " + callId);
            File file = new File("/tmp/call" + callId + ".wav");
            try {
                os = new FileOutputStream(file);
                fileMap.put(callId, os);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (os != null) {
            if (data.length == 0) {
                System.out.println("End callId " + callId);
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                fileMap.remove(callId);
            } else {
                if (data.length != 320)
                    System.out.println("-- oups too short " + callId + " " + data.length);
                try {
                    os.write(data);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lost lease, so terminating.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
            log.info("Reached shard end checkpointing.");
            shardEndedInput.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            log.error("Exception while checkpointing at shard end. Giving up.", e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Scheduler is shutting down, checkpointing.");
        checkpoint(shutdownRequestedInput.checkpointer());

    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        log.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            log.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            log.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

}
