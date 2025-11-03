cd .class
java -classpath "../.lib;." --module-path "../.lib" --add-modules "kafka.streams,kafka.clients,org.apache.commons.pool2" anonymizer/Anonymizer %*
