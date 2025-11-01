package anonymizer;

import java.util.logging.*;
import java.nio.*;
import org.capnproto.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
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


public class ProcessingRunnable implements Runnable {
	private static final Logger log = Logger.getLogger(ProcessingRunnable.class.getName());
	private final DAO<ConsumerRecord<Long, byte[]>> fakeCache;
	private final DAO<List<Object[]>> clickHouseDAO;

	public ProcessingRunnable(final DAO<ConsumerRecord<Long, byte[]>> cache, final DAO<List<Object[]>> click){
		fakeCache = cache;
		clickHouseDAO = click;
	}

	@Override
	public void run(){
		while (true) {
			try {
				TimeUnit.MINUTES.sleep(1);

				final var batchInsertBuffer = new LinkedList<ConsumerRecord<Long, byte[]>>();
				ConsumerRecord<Long, byte[]> current;

				while((current = fakeCache.queryNoWait()) != null){
					if (current == null)
						continue;

					batchInsertBuffer.add(current);
				}

				if(clickHouseDAO.save(batchInsertBuffer.stream().map(this::transformConsumerRecord).toList()) == null)
					batchInsertBuffer.forEach(fakeCache::save);
			}
			catch (final Exception exception) {
				log.throwing(Anonymizer.class.getName(), "main", exception);
			}
		}
	}

	private Object[] transformConsumerRecord(final ConsumerRecord<Long, byte[]> record) {
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

	private String anonymizeAddress(String address) {
		return address.substring(0, address.lastIndexOf(".") + 1).concat("X");
	}
}
