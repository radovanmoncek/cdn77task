package anonymizer;

import java.util.logging.*;
import java.nio.*;
import java.util.concurrent.*;
import java.util.*;

import org.capnproto.*;

/**
  * This Runnable is to be run from within a Thread that is responsible for processing incoming Kafka data, decoding them from Cap'n Proto fromat, anonymizing the remoteAddr, and storing them into ClickHouse.
  */
public class ProcessingRunnable implements Runnable {
	private final int RATE_LIMIT_SLEEP_SECONDS = 90;
	private static final Logger log = Logger.getLogger(ProcessingRunnable.class.getName());
	private final DAO<byte[]> cache;
	private final DAO<List<Object[]>> clickHouseDAO;

	public ProcessingRunnable(final DAO<byte[]> cache, final DAO<List<Object[]>> click){
		log.getParent().setLevel(Level.ALL);
		
		final var handlers = log.getParent().getHandlers();

		for(var i = 0; i < handlers.length; ++i){
			handlers[0].setLevel(Level.ALL);
		}

		this.cache = cache;
		clickHouseDAO = click;
	}

	/**
	 * This method is responsible for processing incoming data from Redis, and inserting them into ClickHouse.
	 * Performs a "pseudo" transaction by re-inserting the unprocessed logs back into the blocking queue.
	 * Event loop is being run.
	 * Runs nested loop, worst time complexity should be O(N^2)
 	 */
	@Override
	public void run(){
		while (true) {
			try {
				TimeUnit.MINUTES.sleep(RATE_LIMIT_SLEEP_SECONDS);

				final var batchInsertBuffer = new LinkedList<byte[]>();
				byte[] current;

				while((current = cache.queryNoWait()) != null){
					if (current == null)
						continue;

					batchInsertBuffer.add(current);
				}

				if(clickHouseDAO.save(batchInsertBuffer.stream().map(this::transformConsumerRecord).toList()) == null)
					batchInsertBuffer.forEach(cache::save);
			}
			catch (final Exception exception) {
				log.throwing(Anonymizer.class.getName(), "main", exception);
			}
		}
	}

	/**
	  * This method decodes the Cap'n Proto encoded data as Object[] for ClickHouse insertion.
	  */
	private Object[] transformConsumerRecord(final byte[] record) {
		try {
			final var message = org.capnproto.Serialize.read(new ArrayInputStream(ByteBuffer.wrap(record)));
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

	/**
	  * Performs the remoteAddr anonymization, using last octet censoring.
	  * Address must be in valid decimal format: 1octet.2octet.3octet.4octet, the method will behave unexpectedly otherwise.
	  */
	private String anonymizeAddress(String address) {
		return address.substring(0, address.lastIndexOf(".") + 1).concat("X");
	}
}
