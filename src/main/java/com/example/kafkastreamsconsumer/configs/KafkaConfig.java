package com.example.kafkastreamsconsumer.configs;

import com.example.kafkastreamsconsumer.collaborators.models.GroupByKey;
import com.example.kafkastreamsconsumer.collaborators.models.Invoice;
import com.example.kafkastreamsconsumer.collaborators.models.InvoiceAggregate;
import com.example.kafkastreamsconsumer.collaborators.models.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@Slf4j
public class KafkaConfig {
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Bean
    public Properties properties() {
//        Map<String,Object> map = new HashMap<>();
//        KafkaStreamsConfiguration kafkaStreamsConfiguration = new KafkaStreamsConfiguration(map);
//        kafkaStreamsConfiguration.asProperties();

        Properties properites = new Properties();
        properites.put(APPLICATION_ID_CONFIG, "kafka-producer-app-id");
        properites.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return properites;
    }

    @Bean
    public Topology topology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<Windowed<GroupByKey>, InvoiceAggregate> kStream = streamsBuilder
                .stream("invoice", Consumed.with(LONG_SERDE, new JsonSerde<Invoice>(Invoice.class)))
                .peek((k,v) -> log.info("=> K: {}, V: {}", k, v))
                .groupBy((k,v) -> new GroupByKey(v.getId(), v.getDesc()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1)))
                .aggregate(() -> new InvoiceAggregate(),
                           (k,v,agg) -> {
                                agg.setDesc(agg.getDesc() + v.getDesc());
                                return agg;
                           },
                           (k,v,agg) -> agg,
                           Materialized.with(new JsonSerde<GroupByKey>(GroupByKey.class), new JsonSerde<InvoiceAggregate>(InvoiceAggregate.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((k,v) -> log.info("==> K: {}, V: {}", k, v));

//                kStream.to( "out-invoice"
//                           ,Produced.with(new JsonSerde<GroupByKey>(GroupByKey.class)
//                                          ,new JsonSerde<InvoiceAggregate>(InvoiceAggregate.class)));

        return streamsBuilder.build();
    }
}