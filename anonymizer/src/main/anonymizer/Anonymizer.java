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
import com.clickhouse.client.api.metrics.*;

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
		final var props = new Properties();
		final var streamsBuilder = new StreamsBuilder(); 

		log.setLevel(Level.ALL);
		log.getParent().getHandlers()[0].setLevel(Level.ALL);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "data-engineering-task-reader");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

		try(final var consumer = new KafkaConsumer<Long, byte[]>(props); final var client = new Client.Builder().addEndpoint("http://127.0.0.1:8124").setUsername("default").setPassword("").compressServerResponse(true).build()){
			final DAO<ConsumerRecord<Long, byte[]>> fakeCache = new CacheDAO();
			final DAO<List<Object[]>> clickHouseDAO = new ClickHouseDAO(client);

			new CLIProcessor(clickHouseDAO).processArgs(args);

			log.info("Anonymizer running");
			consumer.subscribe(Arrays.asList("http_log"));
			//client.execute("create table if not exists HttpLogRecord (timestamp DateTime, resource_id UInt64, bytes_sent UInt64, request_time_milli UInt64, response_status UInt16, cache_status LowCardinality(String), method LowCardinality(String), remote_addr String, url String) engine = MergeTree() ORDER BY (remote_addr, resource_id);");

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
			log.info(object.toString());
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
				.append("insert into http_log values ");

			for (var i = 0; i < objects.size(); ++i) {
				log.finest(String.format("Appending %s to Clickhouse execution DML", Arrays.toString(objects.get(i))));
				sQL
					.append(Arrays.toString(objects.get(i)).replace("[", "(").replace("]", ")"))
					.append(i == objects.size() - 1? ";" : ", ");
			}

			try {
				final var future = client.execute(sQL.toString());
				final var commandResponse = future.get();

				log.info(commandResponse.getMetrics().toString());
				log.info("Server took " + commandResponse.getServerTime());
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

		public String queryWithPrettyPrint(final String sQL){
			final var future = client.queryRecords(sQL);

			try(final var response = future.get(3, TimeUnit.SECONDS)) {
				log.info(sQL);

				final var result = new StringBuilder();
				//final var in = response.getInputStream();
				//final var reader = client.newBinaryFormatReader(response);
				//final var clientTime = response.getMetrics().getMetric(ClientMetrics.OP_DURATION);
				final var totalRows = response.getResultRows();

				log.info(response.getMetrics().toString());
				result.append(String.format("Operation took: %dms\r\n\r\n", response.getServerTime()));
				//result.append(String.format("Operation took: %dms\r\n\r\n", clientTime.getLong()));
				//result.append(String.format("Total rows: %d\r\n", response.getMetrics().getMetric(ServerMetrics.RESULT_ROWS).getLong()));
				result.append(String.format("Total rows: %d\r\n", totalRows));
				result.append(String.format("Total bytes: %d\r\n", response.getMetrics().getMetric(ServerMetrics.NUM_BYTES_READ).getLong()));
				result.append(String.format("Time complexity of row: %fμs\r\n", response.getMetrics().getMetric(ServerMetrics.RESULT_ROWS).getLong()/(float)response.getMetrics().getMetric(ServerMetrics.ELAPSED_TIME).getLong()));
				result.append(String.format("Space complexity of row: %fb\r\n", Math.floor(totalRows/Math.min(-1, response.getMetrics().getMetric(ServerMetrics.NUM_BYTES_READ).getLong()))));
				result.append("\r\n");

				//while(reader.hasNext()){
				for(final var reader : response){
					//reader.next();
					//result.append(String.format("%s %s %s %s\r\n", reader.getInteger("count(resource_id)") + "",reader.getInteger("count(remote_addr)") + "",reader.getInteger("response_status") + "",reader.getInteger("count(cache_status)") + ""));
					result.append(String.format("%s Requests: %s\r\n", reader.getObject(/*"response_status"*/3) + "", reader.getInteger(1) + ""));
				}

				return result.toString();
				}
			catch(final Exception exception) {
				log.throwing(Anonymizer.class.getName(), "queryWithPrettyPrint", exception);

				return new String();
			}
			}

			public void ping() {
				log.info(client.ping() + "");
			}
		}

		private static class CLIProcessor {
			final ClickHouseDAO clickHouseDAO;

			public CLIProcessor(final DAO<List<Object[]>> click){
				clickHouseDAO = (ClickHouseDAO) click;
			}

			public void processArgs(final String[] args){
				if (args.length == 0){
					System.out.println("Invalid ammount of arguments provided.");
					displayHelp();
					System.exit(1);

					return;
				}

				switch(args[0]){
					case "process" ->
						{return;}

					case "--version" -> {
	displayVersion();
					}

					case "--help" -> {
	displayHelp();
					}

					case "stats" -> {
	try {
		System.out.println("Checking client status");
		clickHouseDAO.ping();
		System.out.println("Gathering stats ...");
		TimeUnit.SECONDS.sleep(70);
		System.out.println("Rsponse status stats");
		System.out.println(clickHouseDAO.queryWithPrettyPrint("select count(resource_id), count(remote_addr), response_status, count(cache_status) from stats_view group by response_status;"));
		System.out.println("Gathering stats ...");
		TimeUnit.SECONDS.sleep(70);
		System.out.println("Cache status stats");
		System.out.println(clickHouseDAO.queryWithPrettyPrint("select count(resource_id), count(remote_addr), cache_status, count(response_status) from stats_view group by cache_status;"));
		System.exit(0);
	}
	catch (final Exception exception) {
		log.throwing(Anonymizer.class.getName(), "processArgs", exception);
	}
					}
				}
			}

			private void displayHelp() {
				System.out.println("Anonymizer usage:\r\n");
				System.out.println("run.bat <OPTION>|<process|stats>\r\n");
				System.out.println("OPTION:");
				System.out.println("--help displays this message");
				System.out.println("--version prints version and exits");
				System.exit(0);
			}

			private void displayVersion() {
				System.out.println("Anonymizer version 1.0");
				System.exit(0);
			}
		}
	}
