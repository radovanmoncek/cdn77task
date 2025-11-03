package anonymizer;

import java.util.logging.*;
import java.util.*;
import java.io.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

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

	public static void main(final String[] args) throws Exception {
		final var envFileStream = new FileInputStream(".." + File.separator + ".env");
		final var envProperties = new java.util.Properties();
		
		envProperties.load(envFileStream);

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
				final var client = new Client.Builder().addEndpoint(envProperties.getProperty("CLICKHOUSE_ENDPOINT")).setUsername(envProperties.getProperty("CLICKHOUSE_USERNAME")).setPassword("").compressServerResponse(true).build();
				final var jedis = new UnifiedJedis(envProperties.getProperty("REDIS_URL"))
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
		finally{
			envFileStream.close();
		}
	}
}
