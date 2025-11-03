package anonymizer;

import java.util.logging.*;
import java.util.concurrent.*;
import java.util.*;

import com.clickhouse.client.api.*;
import com.clickhouse.client.api.metrics.*;

/**
  * DAO for ClickHouse database access.
  * Thread safety is NOT provided.
  */
public class ClickHouseDAO implements DAO<List<Object[]>> {
	private static final Logger log = Logger.getLogger(ClickHouseDAO.class.getName());
	private final Client client;

	public ClickHouseDAO(final Client client) {
		log.getParent().setLevel(Level.ALL);
		
		final var handlers = log.getParent().getHandlers();

		for(var i = 0; i < handlers.length; ++i){
			handlers[0].setLevel(Level.ALL);
		}	
		
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

	@Override
	public List<Object[]> query(int iD) throws Exception {
		return null;
	}

	@Override
	public List<Object[]> queryNoWait() throws Exception {
		return null;
	}

	/**
	  * Method that handles the `stats` argument.
	  */
	public String queryWithPrettyPrint(final String sQL){
		final var future = client.queryRecords(sQL);

		try(final var response = future.get(3, TimeUnit.SECONDS)) {
			log.info(sQL);

			final var result = new StringBuilder();
			final var totalRows = response.getResultRows();

			log.info(response.getMetrics().toString());
			result.append(String.format("Operation took: %dms\r\n\r\n", response.getServerTime()));
			result.append(String.format("Total rows: %d\r\n", totalRows));
			result.append(String.format("Total bytes: %d\r\n", response.getMetrics().getMetric(ServerMetrics.NUM_BYTES_READ).getLong()));
			result.append(String.format("Time complexity of row: %fμs\r\n", response.getMetrics().getMetric(ServerMetrics.RESULT_ROWS).getLong()/(float)response.getMetrics().getMetric(ServerMetrics.ELAPSED_TIME).getLong()));
			result.append(String.format("Space complexity of row: %fb\r\n", Math.floor(totalRows/Math.min(-1, response.getMetrics().getMetric(ServerMetrics.NUM_BYTES_READ).getLong()))));
			result.append("\r\n");

			for(final var reader : response){
				result.append(String.format("%s Requests: %s\r\n", reader.getObject(3) + "", reader.getInteger(1) + ""));
			}

			return result.toString();
		}
		catch(final Exception exception) {
			log.throwing(Anonymizer.class.getName(), "queryWithPrettyPrint", exception);

			return new String();
		}
	}

	/**
	  * Check ClickHouse connection status.
	  */
	public void ping() {
		log.info(client.ping() + "");
	}
}
