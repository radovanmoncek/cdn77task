javac -Xlint:all -g -d .class -classpath .lib --module-path .lib --add-modules kafka.clients,kafka.streams,org.apache.commons.pool2 src\main\anonymizer\*.java
