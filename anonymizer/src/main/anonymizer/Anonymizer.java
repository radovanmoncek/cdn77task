package anonymizer;

import java.util.logging.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.util.*;

import com.clickhouse.client.api.*;

import redis.clients.jedis.UnifiedJedis;

/**
 * Main program entrypoint.
 * References:
 * https://kafka.apache.org/41/documentation/streams/tutorial
 * https://www.baeldung.com/apache-kafka
 */
public class Anonymizer {
	private static final Logger log = Logger.getLogger(Anonymizer.class.getName());

	public static void main(final String[] args) {
		final var props = new Properties();

		log.setLevel(Level.ALL);
		log.getParent().getHandlers()[0].setLevel(Level.ALL);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "data-engineering-task-reader");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

		try(
				final var consumer = new KafkaConsumer<Long, byte[]>(props); 
				final var client = new Client.Builder().addEndpoint("http://127.0.0.1:8124").setUsername("default").setPassword("").compressServerResponse(true).build();
				final var jedis = new UnifiedJedis("redis://default:eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81@localhost:6379")
		   ){
			final DAO<byte[]> cache = new CacheDAO(jedis);
			final DAO<List<Object[]>> clickHouseDAO = new ClickHouseDAO(client);
			
			new CLIProcessor(clickHouseDAO).processArgs(args);

			log.info("Anonymizer running");
			consumer.subscribe(Arrays.asList("http_log"));

			final var bufferingThread = new Thread(new BufferingRunnable(cache, consumer));
			final var processingThread = new Thread(new ProcessingRunnable(cache, clickHouseDAO));

			bufferingThread.start();
			processingThread.start();
			processingThread.join();
		   }
		catch (final Exception exception) {
			log.throwing(Anonymizer.class.getName(), "main", exception);
		}
	}
}
