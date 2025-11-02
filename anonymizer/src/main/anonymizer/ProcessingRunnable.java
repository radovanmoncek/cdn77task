package anonymizer;

import java.util.logging.*;
import java.nio.*;
import java.util.concurrent.*;
import java.util.*;

import org.capnproto.*;

/**
 * Performs a "pseudo" transaction by re-inserting the unprocessed logs back into the blocking queue.
 */
public class ProcessingRunnable implements Runnable {
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

	@Override
	public void run(){
		while (true) {
			try {
				TimeUnit.MINUTES.sleep(1);

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

	private String anonymizeAddress(String address) {
		return address.substring(0, address.lastIndexOf(".") + 1).concat("X");
	}
}
