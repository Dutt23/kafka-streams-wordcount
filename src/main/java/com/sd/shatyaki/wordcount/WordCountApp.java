package com.sd.shatyaki.wordcount;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountApp {
    
    public static void main(String ...agrs) {
        
        
        
        Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-01");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        
       StreamsBuilder builder = new StreamsBuilder();
       KStream<String, String> wordCountInput = builder.stream("word-count-input");
       KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
       .flatMapValues(value -> Arrays.asList(value.split(" ")))
       .selectKey((key, value) -> value)
       .groupByKey()
       .count();
     
//       You need to define your serdes if , it does not match the default value
//       Produced.with(stringSerde, longSerde)
       wordCounts.toStream().to("word-count-output", Produced.valueSerde(Serdes.Long()));
       KafkaStreams streams = new KafkaStreams(builder.build(), configs);
       
       // Aggreegate 
       streams.start();
       
       // Print topology
       System.out.println(builder.build().describe());
       Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    

}
