package anonymizer;

import java.util.logging.*;
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

/**
  * Processes arguments passed / delegated from the main method.
  */
public class CLIProcessor {
	private final int RATE_LIMIT_SLEEP_SECONDS = 90;
	private static final Logger log = Logger.getLogger(CLIProcessor.class.getName());
	final ClickHouseDAO clickHouseDAO;

	public CLIProcessor(final DAO<List<Object[]>> click){
		clickHouseDAO = (ClickHouseDAO) click;
	}

	/**
	  * Performs handling of any supplied arguments.
	  */
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
		TimeUnit.SECONDS.sleep(RATE_LIMIT_SLEEP_SECONDS);
		System.out.println("Rsponse status stats");
		System.out.println(clickHouseDAO.queryWithPrettyPrint("select count(resource_id), count(remote_addr), response_status, count(cache_status) from stats_view group by response_status;"));
		System.out.println("Gathering stats ...");
		TimeUnit.SECONDS.sleep(RATE_LIMIT_SLEEP_SECONDS);
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
		System.out.println("ARGUMENTS:");
                System.out.println("process start processing Kafka data");
		System.out.println("stats display ClickHouse statistics\r\n");
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
