package anonymizer;

import java.util.logging.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.*;
//import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.Arrays;
import java.io.*;
import java.util.concurrent.*;
import java.util.*;


import com.clickhouse.client.api.*;
import com.clickhouse.client.api.metrics.*;


public class BufferingRunnable implements Runnable {
	final DAO<ConsumerRecord<Long, byte[]>> fakeCache;
	private final KafkaConsumer<Long, byte[]> consumer;
	
	public BufferingRunnable(DAO<ConsumerRecord<Long, byte[]>> cache, final KafkaConsumer<Long, byte[]> consumer){
		fakeCache = cache;
		this.consumer = consumer;
	}

	@Override
	public void run(){
		while(true) {
			for (final var record : consumer.poll(Duration.ofMillis(100))) {
				fakeCache.save(record);
			}
		}
	}
}
