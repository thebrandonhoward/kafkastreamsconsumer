package com.example.kafkastreamsconsumer.processor;

import com.example.kafkastreamsconsumer.collaborators.models.GroupByKey;
import com.example.kafkastreamsconsumer.collaborators.models.Invoice;
import com.example.kafkastreamsconsumer.collaborators.models.InvoiceAggregate;
import com.example.kafkastreamsconsumer.collaborators.models.Output;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

@Component
@Slf4j
public class WordCountProcessor {

    @Autowired
    private Topology topology;
    @Autowired
    private Properties properties;

    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<String> STRING_SERDE = Serdes.String();

//    @Autowired
//    void buildPipeline(StreamsBuilder streamsBuilder) {
//        KStream<String, String> messageStream = streamsBuilder
//          .stream("input-topic", Consumed.with(STRING_SERDE, STRING_SERDE));
//
//        KTable<String, Long> wordCounts = messageStream
//          .mapValues((ValueMapper<String, String>) String::toLowerCase)
//          .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
//          .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
//          .count();
//          //.count(Materialized.as("counts"));
//
//        wordCounts.toStream().to("output-topic");
//    }

//    @Autowired
//    void buildPipeline(StreamsBuilder streamsBuilder) {
//        log.info("Starting...");
//        streamsBuilder
//                .stream("input-topic", Consumed.with(STRING_SERDE, STRING_SERDE))
//                .peek((k,v) -> log.info("K: {}, V: {}", k, v))
//                .mapValues((k,v) -> Output.builder().words(v).build())
//                .peek((k,v) -> log.info("K: {}, V: {}", k, v))
//                .to("output-topic", Produced.with(STRING_SERDE, new JsonSerde<>()));
//    }

//    @Autowired
//    void buildPipeline(StreamsBuilder streamsBuilder) {
//        log.info("Starting...");
//        streamsBuilder
//                .stream("invoice", Consumed.with(LONG_SERDE, new JsonSerde<Invoice>()))
//                .peek((k,v) -> log.info("=> K: {}, V: {}", k, v));
//                .groupBy((k,v) -> new GroupByKey(v.getId(), v.getDesc()))
//                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1)))
//                .aggregate(() -> new InvoiceAggregate(),
//                           (k,v,agg) -> {
//                                agg.setDesc(agg.getDesc() + v.getDesc());
//                                return agg;
//                           },
//                            (k,v,agg) -> agg,
//                            Materialized.with(new JsonSerde<GroupByKey>(), new JsonSerde<InvoiceAggregate>()))
//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
//                .toStream()
//                .peek((k,v) -> log.info("==> K: {}, V: {}", k, v))
//                .to("out-invoice", Produced.with(new JsonSerde<>(), new JsonSerde<>()));

//    }

    @PostConstruct
    public void buildPipeline() {
        log.info("Topology starting...");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}