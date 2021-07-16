package com.sd.shatyaki.enrich;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

public class UserEnrichConsumer {

    public static void main(String... args) {

        Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-enrich-01");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        configs.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> userGloabalData = builder.globalTable("user-table");

        KStream<String, String> userPurchseData = builder.stream("user-purchases");

//        Inner join technique
        KStream<String, String> userPurchasesEnrichedJoin = userPurchseData.join(
                userGloabalData, 
                (key, value) -> key,
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + ", UserInfo = [" + userInfo + "]");
        
        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");
        
        // Left join technique
        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchseData.leftJoin(
                userGloabalData,
                (key, value) -> key,
                (userPurchase, userInfo) ->{
                    if(userInfo != null)
                        return "Purchase= " + userPurchase + ", UserInfo = [" + userInfo + "]";
                    else 
                        return "Purchase=" + userPurchase + ",UserInfo=null";
                });
        
        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");
        
        KafkaStreams streams = new KafkaStreams(builder.build(), configs);
        streams.cleanUp();
        streams.start();
        
        System.out.println(builder.build().describe());
              
        
    }
}
