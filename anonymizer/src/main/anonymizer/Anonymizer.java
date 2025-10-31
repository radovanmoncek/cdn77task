package anonymizer;

import java.util.logging.*;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.Arrays;
import java.io.*;
import java.nio.*;
import java.util.concurrent.*;
import java.util.*;

import org.capnproto.*;

import com.clickhouse.client.api.*;

/**
 * Main program entrypoint.
 * Performs a "pseudo" transaction by re-inserting the unprocessed logs back into the blocking queue.
 * References:
 * https://kafka.apache.org/41/documentation/streams/tutorial
 * https://www.baeldung.com/apache-kafka
 */
public class Anonymizer {
	private static final Logger log = Logger.getLogger(Anonymizer.class.getName());

	public static void main(String[] args) {
		log.setLevel(Level.ALL);
		log.getParent().getHandlers()[0].setLevel(Level.ALL);
		log.info("Anonymizer running");

		final var props = new Properties();
		final var streamsBuilder = new StreamsBuilder(); 

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "data-engineering-task-reader");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

		try(final var consumer = new KafkaConsumer<Long, byte[]>(props); final var client = new Client.Builder().addEndpoint("http://127.0.0.1:8124").setUsername("default").setPassword("").build()){
			consumer.subscribe(Arrays.asList("http_log"));
			client.execute("create table if not exists HttpLogRecord (timestamp DateTime, resource_id UInt64, bytes_sent UInt64, request_time_milli UInt64, response_status UInt16, cache_status LowCardinality(String), method LowCardinality(String), remote_addr String, url String) engine = MergeTree() ORDER BY (remote_addr, resource_id);");

			final DAO<ConsumerRecord<Long, byte[]>> fakeCache = new CacheDAO();
			final DAO<List<Object[]>> clickHouseDAO = new ClickHouseDAO(client);
			final var bufferingThread = new Thread(() -> {
				while(true) {
					for (final var record : consumer.poll(Duration.ofMillis(100))) {
						fakeCache.save(record);
					}
				}
			});
			final var processingThread = new Thread(() -> {
				while (true) {
					try {
						TimeUnit.MINUTES.sleep(1);

						final var batchInsertBuffer = new LinkedList<ConsumerRecord<Long, byte[]>>();
						ConsumerRecord<Long, byte[]> current;

						while((current = fakeCache.queryNoWait()) != null){
							//final var current = fakeCache.query(-1);

							if (current == null)
								continue;

							batchInsertBuffer.add(current);
						}

						if(clickHouseDAO.save(batchInsertBuffer.stream().map(Anonymizer::transformConsumerRecord).toList()) == null)
							//fakeCache.addAll(0, batchInsertBuffer);
							batchInsertBuffer.forEach(fakeCache::save);
					}
					catch (final Exception exception) {
						log.throwing(Anonymizer.class.getName(), "main", exception);
					}
				}
			});

			bufferingThread.start();
			processingThread.start();
			processingThread.join();
		}
		catch (final Exception exception) {
			log.throwing(Anonymizer.class.getName(), "main", exception);
		}
	}

	private static Object[] transformConsumerRecord(final ConsumerRecord<Long, byte[]> record) {
		try {
			final var message = org.capnproto.Serialize.read(new ArrayInputStream(ByteBuffer.wrap(record.value())));
			final var httpLogRecord = message.getRoot(HttpLogRecordOuter.HttpLogRecord.factory);
		return new Object[]{
			httpLogRecord.getTimestampEpochMilli(),
				httpLogRecord.getResourceId(),
				httpLogRecord.getBytesSent(),
				httpLogRecord.getRequestTimeMilli(),
				httpLogRecord.getResponseStatus(),
				"'".concat(httpLogRecord.getCacheStatus().toString()).concat("'"),
				"'".concat(httpLogRecord.getMethod().toString()).concat("'"),
				"'".concat(anonymizeAddress(httpLogRecord.getRemoteAddr().toString())).concat("'"),
				"'".concat(httpLogRecord.getUrl().toString()).concat("'")
		};
		}
		catch (final Exception exception) {
			log.throwing(Anonymizer.class.getName(), "transformConsumerRecord", exception);

			return new Object[0];
		}
	}

	private static String anonymizeAddress(String address) {
		return address.substring(0, address.lastIndexOf(".") + 1).concat("X");
	}

	private interface DAO<T> {
		T save(T object);
		T query(int id) throws Exception;
		T queryNoWait() throws Exception;
	}

	/**
	 * This DAO class should encapsulate a thread safe cache implementation for immediate Kafka topic data buffering.
	 */
	private static class CacheDAO implements DAO<ConsumerRecord<Long, byte[]>> {
		private final LinkedBlockingQueue<ConsumerRecord<Long, byte[]>> fakeCache = new LinkedBlockingQueue<>();

		@Override
		public ConsumerRecord<Long, byte[]> save(ConsumerRecord<Long, byte[]> object) {
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

	private static class ClickHouseDAO implements DAO<List<Object[]>> {
		private final Client client;

		public ClickHouseDAO(final Client client) {
			this.client = client;
		}

		@Override
		public List<Object[]> save(List<Object[]> objects) {
			log.finest(objects.toString());

			final var sQL = new StringBuilder()
				.append("insert into HttpLogRecord values ");

			for (var i = 0; i < objects.size(); ++i) {
				log.finest(String.format("Appending %s to Clickhouse execution DML", Arrays.toString(objects.get(i))));
				sQL
					.append(Arrays.toString(objects.get(i)).replace("[", "(").replace("]", ")"))
					.append(i == objects.size() - 1? ";" : ", ");
			}

			//final var future = client.execute("select * from HttpLogRecord;");

			try {
				final var future = client.execute(sQL.toString());
				final var commandResponse = future.get();

				log.info(commandResponse.getMetrics().toString());
			}
			catch (final Exception exception) {
				log.throwing(Anonymizer.class.getName(), "save", exception);

				return null;
			}

			return objects;
		}

		public List<Object[]> query(int iD) throws Exception {
			return null;
		}

		public List<Object[]> queryNoWait() throws Exception {
			return null;
		}
	}
}
