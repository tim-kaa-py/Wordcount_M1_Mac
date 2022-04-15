package org.firstkafkastream.com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;


import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-application");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //Run two instances: set two different state config location (otherwise it crashes)
        //config.setProperty(StreamsConfig.STATE_DIR_CONFIG,"/Users/tim/IdeaProjects/WordCount/src/main/java/org/firstkafkastream/com/state1");
        //config.setProperty(StreamsConfig.STATE_DIR_CONFIG,"/Users/tim/IdeaProjects/WordCount/src/main/java/org/firstkafkastream/com/state2");

        StreamsBuilder builder = new StreamsBuilder();
        // 1) stream

        KStream<String, String> wordCountInput = builder.stream("word-count-input"); //input topic
        // <key, value>


        KTable<String, Long> wordCounts = wordCountInput
                // 2) map values lower key
                .mapValues(textLine -> textLine.toLowerCase())
                // 3) split by space (flatmap)
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split(" ")))
                // 4) set key  to value
                .selectKey((ignoredKey, word) -> word)
                // 5) group by key
                .groupByKey()
                // 6) count occourneces
                .count(Named.as("Counts"));



        // 7) write result back to Kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(),config);
        streams.start();

        //Print topology
        System.out.println(streams.toString());

        // shutdown hook to close streams correctly (last line in every code)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
