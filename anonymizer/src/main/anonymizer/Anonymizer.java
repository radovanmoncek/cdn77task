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

import org.capnproto.*;

/**
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

		try(final var consumer = new KafkaConsumer<Long, byte[]>(props)){
			consumer.subscribe(Arrays.asList("http_log"));

			final DAO<ConsumerRecord<Long, byte[]>> fakeCache = new CacheDAO();
			final DAO<Object[]> clickHouseDAO = new ClickHouseDAO();
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
						final var current = fakeCache.query(-1);

						if (current == null)
							continue;

						final var message = org.capnproto.Serialize.read(new ArrayInputStream(ByteBuffer.wrap(current.value())));
						final var httpLogRecord = message.getRoot(HttpLogRecordOuter.HttpLogRecord.factory);

						clickHouseDAO.save(new Object[] {
							httpLogRecord.getTimestampEpochMilli(),
								httpLogRecord.getResourceId(),
								httpLogRecord.getBytesSent(),
								httpLogRecord.getRequestTimeMilli(),
								httpLogRecord.getResponseStatus(),
								httpLogRecord.getCacheStatus(),
								httpLogRecord.getMethod(),
								anonymizeAddress(httpLogRecord.getRemoteAddr().toString()),
								httpLogRecord.getUrl().toString()
						});
					}
					catch (final Exception exception) {
						log.throwing(Anonymizer.class.getName(), "main", exception);
					}
				}
			});

			bufferingThread.start();
			processingThread.start();
		}
		catch (final Exception exception) {
			log.throwing(Anonymizer.class.getName(), "main", exception);
		}
	}

	private static String anonymizeAddress(String address) {
		return address.substring(0, address.lastIndexOf(".") + 1).concat("X");
	}

	private interface DAO<T> {
		T save(T object);
		T query(int id) throws Exception;
	}

	private static class CacheDAO implements DAO<ConsumerRecord<Long, byte[]>> {
		private final LinkedBlockingQueue<ConsumerRecord<Long, byte[]>> fakeCache = new LinkedBlockingQueue<>();

		public ConsumerRecord<Long, byte[]> save(ConsumerRecord<Long, byte[]> object) {
			fakeCache.add(object);

			return object;
		}

		public ConsumerRecord<Long, byte[]> query(int ignored) throws Exception {
			return fakeCache.poll(10, TimeUnit.MILLISECONDS);
		}
	}

	private static class ClickHouseDAO implements DAO<Object[]> {
		public Object[] save(Object[] object) {
			log.info(Arrays.toString(object));

			return object;
		}

		public Object[] query(int iD) throws Exception {
			return null;
		}
	}
}
