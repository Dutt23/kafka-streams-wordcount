package com.sd.shatyaki.wordcount;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class FavouriteColorApp {

    
    public static void main(String ...agrs) {
        Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-01");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        
        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        configs.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> favouriteColor = builder.stream("favourite-color-input");
        
        
        KTable<String, String> userColorTable = favouriteColor.filter((key, value) -> value.contains(","))
        .selectKey((key, value)-> value.split(",")[0])
        .mapValues(value -> value.split(",")[1])
        .filter((key, value) -> Arrays.asList("red", "blue", "green").contains(value)).toTable();
        
        
        
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        
//        Used to create changelog
        // Creates reparition file with this name
        KTable<String, Long> favouriteColours = userColorTable.groupBy((user, color) -> new KeyValue<>(color, color)).count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                .withKeySerde(stringSerde)
                .withValueSerde(longSerde));
        
       
        favouriteColours.toStream().to("favourite-colour-output", Produced.with(Serdes.String(),Serdes.Long()));
        
        
        KafkaStreams streams = new KafkaStreams(builder.build(), configs);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
