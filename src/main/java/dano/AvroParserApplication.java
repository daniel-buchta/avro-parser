package dano;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AvroParserApplication {

  private static final String SCHEMA =
      "{ \"type\" : \"record\", \"name\" : \"twitter_schema\", \"namespace\" : \"com.miguno.avro\", "
          + "\"fields\" : [ "
          + "{ \"name\" : \"username\", \"type\" : \"string\", \"doc\"  : \"Name of the user account on Twitter.com\" }, "
          + "{ \"name\" : \"tweet\", \"type\" : [\"null\", \"string\"], \"doc\"  : \"The content of the user's Twitter message\" }, "
          + "{ \"name\" : \"timestamp\", \"type\" : \"long\", \"doc\"  : \"Unix epoch time in seconds\" } ], \"doc:\" : \"A basic schema for storing Twitter messages\" }";

  public static void main(String[] args) throws IOException {

    String avroJson1 = "123456   {\"username\":\"miguno\",\"tweet\": {\"string\": \"Rock: Nerf paper, scissors is fine.\"},\"timestamp\": 1366150681 }";
    String avroJson2 = "123456   {\"username\":\"miguno\",\"tweet\": null,\"timestamp\": 1366150681 }";

    parse(SCHEMA, Stream.of(avroJson1, avroJson2)).forEach(System.out::println);

    try (Stream<String> stream = Files.lines(Paths.get("src/test/resources/test.data"))) {
      new KafkaAVroProducer().send(parse(SCHEMA, stream));
    }
  }

  private static Stream<Record<GenericRecord>> parse(String schema, Stream<String> stream) {

    Pattern r = Pattern.compile("(\\d+)(.*)");
    AvroParser parser = new AvroParser(schema);

    return stream.map(r::matcher).filter(Matcher::find)
        .map(m -> Record.of(m.group(1), m.group(2)))
        .map(m -> m.withValue(parser.parse(m.value)));
  }


  @Value(staticConstructor = "of")
  static class Record<V> {
    String key;

    V value;

    <N> Record<N> withValue(N newValue) {
      return Record.of(key, newValue);
    }
  }

  @Slf4j
  static class AvroParser {

    private final DecoderFactory decoderFactory = new DecoderFactory();

    private final Schema schema;

    private final GenericDatumReader<GenericRecord> avroReader;

    AvroParser(String schema) {
      this.schema = new Schema.Parser().parse(schema);
      this.avroReader = new GenericDatumReader<>(this.schema);
    }

    GenericRecord parse(String avroJson) {
      try {
        Decoder decoder = decoderFactory.jsonDecoder(schema, avroJson);
        return avroReader.read(null, decoder);
      } catch (IOException e) {
        log.warn("parse({}) failed: {}", avroJson, e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  @Slf4j
  static class KafkaAVroProducer {

    private static final String TOPIC = "todo";
    private static final String BROKERS = "broker1:9092,broker2:9092";
    private static final String REGISTRY = "http://broker1:8081";

    private final Producer<String, GenericRecord> producer;// schema registry url.

    KafkaAVroProducer() {
      this.producer = new KafkaProducer<>(createConfigProperties(BROKERS, REGISTRY));
    }

    void send(Stream<Record<GenericRecord>> records) {

      AtomicLong successCount = new AtomicLong();
      AtomicLong errorCount = new AtomicLong();

      Callback postSender = (recordMetadata, e) -> {
        if (e != null) {
          log.error("Error adding to topic: {}", e.getMessage());
          errorCount.incrementAndGet();
        } else {
          successCount.incrementAndGet();
        }
      };

      long produced = records.map(r -> new ProducerRecord<>(TOPIC, r.key, r.value))
          .map(r -> producer.send(r, postSender))
          .count();

      while (successCount.get() + errorCount.get() < produced) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          log.warn("send interrupted: {}", e.getMessage());
          Thread.currentThread().interrupt();
        }
      }

      if (successCount.get() == produced) {
        log.info("Successfully send {} records", produced);
      } else {
        log.warn("Send of {}/{} records faield", errorCount.get(), produced);
      }
    }

    private static Properties createConfigProperties(String brokers, String registry) {
      Properties p = new Properties();
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());
      p.put(ProducerConfig.ACKS_CONFIG, "all");
      p.put(ProducerConfig.RETRIES_CONFIG, "10");
      p.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
      p.put(ProducerConfig.LINGER_MS_CONFIG, "1");
      p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
      return p;
    }
  }

}
