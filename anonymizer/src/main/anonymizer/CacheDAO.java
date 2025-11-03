package anonymizer;

import org.apache.kafka.clients.consumer.*;

import java.util.concurrent.*;
import java.util.*;
import java.util.logging.*;

import redis.clients.jedis.UnifiedJedis;

/**
 * This DAO class encapsulates a thread safe cache implementation for immediate Kafka topic data buffering.
 */
public class CacheDAO implements DAO<byte[]> {
	private final Logger log = Logger.getLogger(CacheDAO.class.getName());
	private final UnifiedJedis jedis;
	private final LinkedBlockingQueue<String> backingQueue = new LinkedBlockingQueue<>();

	public CacheDAO(final UnifiedJedis jedis){
		this.jedis = jedis;

		log.info("Auto loaded Redis keys " + String.valueOf(jedis.keys("*")));
		backingQueue.addAll(jedis.keys("*"));
	}

	public ConsumerRecord<Long, byte[]> save(ConsumerRecord<Long, byte[]> object) {
		log.info(object.toString());
		save(object.value());

		return object;
	}

	@Override
	public byte[] save(byte[] array) {
		return accessRedis(null, array);
	}

	@Override
	public byte[] query(int ignored) throws Exception {
		return null;
	}

	@Override
	public byte[] queryNoWait() {
		return accessRedis(backingQueue.poll(), null);
	}

	/**
	  * This is the ONLY method to be used for thread safe Redis access.
	  * Both directions of access are supported (read, write), based on arguments supplied.
	  * The parameters of this method are mutualy exclusive implicitly (supplying both retrieves a value; supplying neither returns null)
	  * @param key the key to be used for reading data, value must be ommited
	  * @param value the value to be used for writing data, key must be ommited
	  * @return identity of value if writting is being performed; retrieved value, if reading took place; null otherwise
	  */
	synchronized private final byte[] accessRedis(final String key, final byte[] value){
		if(key != null){
			log.info("Reading from Redis " + key);
			
			final var tempValue = jedis.get(key);

			jedis.del(key);

			return Base64.getDecoder().decode(tempValue);
		}

		if(value == null)
			return null;

		log.info("Writing to Redis " + Arrays.toString(value));

		jedis.set(Arrays.hashCode(value) + "", Base64.getEncoder().encodeToString(value));
		backingQueue.offer(Arrays.hashCode(value) + "");

		return value;
	}
}
