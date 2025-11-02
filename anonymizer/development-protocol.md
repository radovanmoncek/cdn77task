# cdn77 Data engineering task protocol

# Summary

Problem: Because of the GDPR regulations you have to anonymize the client IP. For each record change remoteAddr's last octet to X before sending it over to ClickHouse (e.g. 1.2.3.4 -> 1.2.3.X).

Solution: This is done during pre-processing of Cap'n Proto decoded data. The String is redacted as per aforementioned instructions.

Problem: Each record must be stored to ClickHouse, even in the event of network or server error. Make sure that you handle those appropriately.

Solution: The data processing business logic is split into two Thread instances. The first thread is responsible only for storing any incoming data into persistent temporary storage.
Redis was chosen for this task, as it is very performant, allows easy persistence control, and is able to run in a Docker container. LinkedBlockingQueue is also utilized as a backing data structure to introduce some abstraction. Thread safeness is achieved using a synchronized method which must be used to access Redis during runtime. Actual data processing happens from within the second thread, also acting as a ClickHouse upstream. If there is a network error during ClickHouse insertion, the data is automatically reinserted into the temporary queue. These approaches, in tandem, provide a fairly robust anti-loss data gathering process. The only instance in which data loss could occur will be the 20 second gab between Redis saves, or a crash of the entire application during transfer from Redis to ClickHouse (could be solved by only removing from Redis after successful ClickHouse insertion).

Problem: Your application should communicate with the ClickHouse server only through the proxy which has rate limiting for a 1 request per minute limit.

Solution: Only the 8124 TCP port is used to connect to ClickHouse. Rate limit is handled with TimeUnit and sleep() with try catch. This approach is not optimal, and could be, in the future, replaced with a try mechanism, or availbility checking.

Problem: If there are any limitation about your application write down what they are, and how would you solve them in the future. For example What is the latency of the data? Is there any scenario when you will start losing data? Is it possible that there will be a stored duplicate record?

Solution: Some of these were already described in solutions above. The data latency depends on heavy components of the processing chain (namely Kafka, Redis, Cap'n Proto, ClickHouse, etc.). Java, and the underlying code should have minimal impact on performance, having, at worst, an O(n^2) loop. Logging is also provided, indicating latency in the range of microseconds, milliseconds at worst. The topic of data loss was described, in depth, above. Chances of record duplicity are very low, given that provided data contains two candidate key columns (address, and resource_id). Redis will also override any existing key. The only chance of storing a duplicate record would come in the form of ClickHouse non-immediate deduplication. (https://clickhouse.com/docs/guides/developer/deduplication). Another limitation of the presented program is a lacking OOP design that could definitely be improved, and built uppon (service for DAO, etc.) Also, the rate-limit solution is far from optimal.

Problem: You can implement the task in any of those languages: Go C/C++ Java Rust

Solution: Java chosen for ease of use, existing experience, support for all required technologies

Problem: Load those data into ClickHouse, using a new table called http_log with the following columns.

Solution: Required table was created, and successfully populated.

Problem: Provide a table with ready made totals of served traffic for any combination of resource ID, HTTP status, cache status and IP address. The totals need to be queried efficiently, in seconds at best, for on-demand rendering of traffic charts in a front-end such as Grafana.

Solution: View with name `stats_view` was created as a projection of the required columns.

Problem: Characterize the aggregated select query time to show the table architecture is fit for purpose. Provide an estimate of disk space required given average incoming message rate retention of the aggregated data

Solution:

```sql
select * from stats_view;
```

Elapsed: 0.015 sec, read 8.74 thousand rows, 277.27 KB.

|---| Time complexity | Space complexity |
| 1 row | ≈ 131μs | ≈ 31B |

Forma odevzdani je na tobe: zde na GitHubu.

# proc jsi to resil
- Rozumím Javě
- Velmi rychle jsem pochopil podstatu problému
- Problematika mi přišla sympatická

# tak jak jsi to resil

# jak by to slo resit jinak

# jak by to reseni skalovalo
- Jedná se o OOP
- Extenzibilita je poměrně snadně dosažitelná (Výměna databázových technologií, tříd, atp.)
- Neměl by být problém program nasadit na několik serverů (horizontální škálování)

# jak se to bude chovat z hlediska {performance, udrzitelnosti kodu, security, …}
- Kvůli použití Javy nebude výkon srovnatelný se systémovými jazyky jako C/C++, Go, Rust
- Program neprovádí žádné extrémně časově / prostorově náročné operace
- Udržitelnost by neměla být problém (OOP)
- Program je navržen pro běh izolovaný od uživatelů uvnitř infrastruktury, útočnou plochou/vektory budou tedy jednoznačně příchozí data a špátný návrh kódu (buffer overflow, RCE)

# kde to reseni je optimalni a kde naopak neni

#co by slo zlepsit ale proc to right now nezlepsujes

#jak dlouho ti to realne trvalo (research/implementace/debug/…): 
přibližně 5 dní čistého času (do vypracování zasahovala škola);

# na cem ses zaseknul a jak si to vyresil (Where I got stuck?)
- While working with Cap'n Proto
	- It was necessary to use Linux with WSL, in order to compile the Java plugin (would probably prove easier to use C, or C++)
	- I had to use make, and debug for several hours
- Generally during adaptation to client libs for external technologies (ClickHouse, Kafka, etc.)
- ClickHouse also provided some problematic caveats (figuring out the default user, connecting through the 8124 proxy, etc.)
