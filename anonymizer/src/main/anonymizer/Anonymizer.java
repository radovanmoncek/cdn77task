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
		
//final var rootHandler = 
		log.getParent().getHandlers()[0].setLevel(Level.ALL);
		log.info("Anonymizer running");
		
		final var props = new Properties();
		final var streamsBuilder = new StreamsBuilder();
	
		//props.put(StreamsConfig.APPLICATION_ID_CONFIG, "http_anonymization");	
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "data-engineering-task-reader");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		//final var stream = streamsBuilder.stream("http_log");
		//final var topology = streamsBuilder.build();

		//log.info(topology.describe().toString());
		//stream.print(Printed.toSysOut());

		//final var streams = new KafkaStreams(topology, props);

		//streams.start();

		//while(true){}

		try(final var consumer = new KafkaConsumer<Long, byte[]>(props)){
			consumer.subscribe(Arrays.asList("http_log"));

			final var queue = new ConcurrentLinkedQueue<ConsumerRecord<Long, byte[]>>();
			final var bufferingThread = new Thread(() -> {
				while(true) {
					for (final var record : consumer.poll(Duration.ofMillis(100))) {
						queue.add(record);
					}
				}
			});
			final var processingThread = new Thread(() -> {
				while (true) {
				//consumer.poll(Duration.ofMinutes(1)).records("http_log").forEach(record -> {
				try {
					//var bytes = "";

					//for(var i = 0; i < record.value().length; ++i) {
					//log.info(record.value()[i]);
						//bytes += record.value()[i];
					//}

					log.info(bytes);
					
					final var message = org.capnproto.Serialize.read(new ArrayInputStream(ByteBuffer.wrap(/*record.value()*/queue.poll())));
					
					log.info(message.toString());

					final var httpLogRecord = message.getRoot(HttpLogRecordOuter.HttpLogRecord.factory);

					log.info(httpLogRecord.getRemoteAddr().toString());
				}
				catch (final Exception exception) {
					log.throwing(Anonymizer.class.getName(), "main", exception);
				//log.info(Arrays.asList(record.value()).stream().map(value -> "" + value).reduce("", (current, next) -> current + next));
				//log.info(record.value().length() + "");
				//log.info(new String(record.value()));
				//System.out.println(record.value());
				}
			}
			});
		}
		catch (final Exception exception) {
			log.throwing(Anonymizer.class.getName(), "main", exception);
		}
	}
}
