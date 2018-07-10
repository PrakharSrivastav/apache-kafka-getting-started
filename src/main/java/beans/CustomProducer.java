package beans;

import config.AppConfig;
import config.GsonSerializer;
import model.Invoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Properties;
import java.util.UUID;

public final class CustomProducer {

    private final Producer<String, Invoice> kafkaProducer;
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

    public Producer<String, Invoice> getKafkaProducer() { return kafkaProducer; }

    public static void sendMessages(final AppConfig.KafkaConfig config) {
        final Producer<String, Invoice> producer = new CustomProducer(config).getKafkaProducer();
        final AppConfig.KafkaConfig.Topic invoiceTopic = config.topics().get(0);
        logger.info("name {}", invoiceTopic.name());
        producer.initTransactions();
        try {
            producer.beginTransaction();
            Invoice invoice = Invoice.newRandomInvoice();
            logger.info("Started Producing New Invoice :: {}", invoice.toString());
            producer.send(new ProducerRecord<>(
                    invoiceTopic.name(),
                    null,
                    invoice.getInvoiceId(),
                    invoice,
                    getProducerHeaders(invoiceTopic, config))
            );
            producer.commitTransaction();
            producer.flush();
            Thread.sleep(6000);
        } catch (Exception e) {
            logger.error("Error is ", e);
            producer.abortTransaction();
            producer.close();
        }
    }

    private static Headers getProducerHeaders(final AppConfig.KafkaConfig.Topic topic, final AppConfig.KafkaConfig config) {
        final Headers headers = new RecordHeaders();

        headers.add(new RecordHeader("Content-Type", config.contentType().getBytes()));
        headers.add(new RecordHeader("Encoding", config.encoding().getBytes()));
        headers.add(new RecordHeader("Source", topic.source().getBytes()));
        headers.add(new RecordHeader("Target", topic.target().getBytes()));

        return headers;
    }
}
