package beans;

import config.AppConfig;
import config.GsonSerializer;
import model.Invoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public final class CustomProducer {

    private final org.apache.kafka.clients.producer.Producer<String, Invoice> kafkaProducer;
    static Logger logger = LoggerFactory.getLogger(CustomProducer.class);

    public CustomProducer(final AppConfig.KafkaConfig config) {

        logger.info("Kafka configs are ");
        logger.info("Content-Type {}", config.contentType());
        logger.info("Destination {}", config.destination());
        logger.info("Encoding {}", config.encoding());
        logger.info("Topics {}", config.topics());

        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.destination());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "ApacheKafkaGettingStarted");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public Producer<String, Invoice> getKafkaProducer() {
        return kafkaProducer;
    }
}
