package anonymizer;
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


import java.util.logging.*;

/**
 * This DAO class should encapsulate a thread safe cache implementation for immediate Kafka topic data buffering.
 */
public class CacheDAO implements DAO<ConsumerRecord<Long, byte[]>> {
	private final Logger log = Logger.getLogger(CacheDAO.class.getName());
	private final LinkedBlockingQueue<ConsumerRecord<Long, byte[]>> fakeCache = new LinkedBlockingQueue<>();

	@Override
	public ConsumerRecord<Long, byte[]> save(ConsumerRecord<Long, byte[]> object) {
		log.info(object.toString());
		fakeCache.offer(object);

		return object;
	}

	@Override
	public ConsumerRecord<Long, byte[]> query(int ignored) throws Exception {
		return fakeCache.poll(10, TimeUnit.MILLISECONDS);
	}

	@Override
	public ConsumerRecord<Long, byte[]> queryNoWait() {
		return fakeCache.poll();
	}
}
