cd .class
java -classpath "../.lib;." --module-path "../.lib" --add-modules "kafka.streams,kafka.clients" anonymizer/Anonymizer %*
