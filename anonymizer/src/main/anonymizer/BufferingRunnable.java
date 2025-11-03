package anonymizer;

import org.apache.kafka.clients.consumer.*;

import java.util.logging.*;
import java.time.Duration;

/**
 * This is a Runnable to be run from inside of the buffering thread.
 * This Runnable is responsible for saving incoming traffic from Apache Kafka to Redis cache.
 */
public class BufferingRunnable implements Runnable {
	private final DAO<byte[]> cache;
	private final KafkaConsumer<Long, byte[]> consumer;
	
	public BufferingRunnable(final DAO<byte[]> cache, final KafkaConsumer<Long, byte[]> consumer){
		this.cache = cache;
		this.consumer = consumer;
	}

	/**
	  * Buffers incoming traffic from Apacahe Kafka into the Redis DAO.
	  * Runs an event loop with nested loop.
	  * The worst time complexity should be N(n^2)
	  */
	@Override
	public void run(){
		while(true) {
			for (final var record : consumer.poll(Duration.ofMillis(100))) {
				((CacheDAO) cache).save(record);
			}
		}
	}
}
