package anonymizer;

import org.apache.kafka.clients.consumer.*;

import java.util.logging.*;
import java.time.Duration;

/**
 *
 */
public class BufferingRunnable implements Runnable {
	private final DAO<byte[]> cache;
	private final KafkaConsumer<Long, byte[]> consumer;
	
	public BufferingRunnable(final DAO<byte[]> cache, final KafkaConsumer<Long, byte[]> consumer){
		this.cache = cache;
		this.consumer = consumer;
	}

	@Override
	public void run(){
		while(true) {
			for (final var record : consumer.poll(Duration.ofMillis(100))) {
				((CacheDAO) cache).save(record);
			}
		}
	}
}
