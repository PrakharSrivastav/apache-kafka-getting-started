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

import java.util.Properties;
import java.util.UUID;

public final class CustomProducer {

    private static Producer<String, Invoice> kafkaProducer;
    private static Boolean producerExists = false;
    private static Logger logger = LoggerFactory.getLogger(CustomProducer.class);

    // Instantiate a MessageProducer
    public static void init(final AppConfig.KafkaConfig config) {
        // If producer was already created
        if (!producerExists) {
            logger.info("Instantiating a new Producer");

            // configure consumer properties and setup consumer
            final Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.destination());
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "ApacheKafkaGettingStarted");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class);
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
            kafkaProducer = new KafkaProducer<>(properties);
            producerExists = true;
        } else {
            kafkaProducer = kafkaProducer();
        }

        // Initialize kafka transaction manager
        kafkaProducer().initTransactions();
    }

    private static Producer<String, Invoice> kafkaProducer() { return kafkaProducer; }

    public static void sendMessages(final AppConfig.KafkaConfig config) {
        try {
            kafkaProducer().beginTransaction();
            kafkaProducer().send(producerRecord(config));
            kafkaProducer().commitTransaction();
            kafkaProducer().flush();
            Thread.sleep(1000);
        } catch (Exception e) {
            logger.error("Error is ", e);
            kafkaProducer().abortTransaction();
            kafkaProducer().close();
        }
    }

    // Create a kafkaProducer record
    private static ProducerRecord<String, Invoice> producerRecord(final AppConfig.KafkaConfig config) {
        final AppConfig.KafkaConfig.Topic topic = config.topics().get(0);
        final Invoice invoice = Invoice.newRandomInvoice();
        logger.info("Producing {}", invoice.toString());
        return new ProducerRecord<>(topic.name(), null, System.currentTimeMillis(), invoice.getInvoiceId(), invoice, headers(topic, config));
    }

    // Setup message headers
    private static Headers headers(final AppConfig.KafkaConfig.Topic topic, final AppConfig.KafkaConfig config) {
        final Headers headers = new RecordHeaders();

        headers.add(new RecordHeader("Content-Type", config.contentType().getBytes()));
        headers.add(new RecordHeader("Encoding", config.encoding().getBytes()));
        headers.add(new RecordHeader("Source", topic.source().getBytes()));
        headers.add(new RecordHeader("Target", topic.target().getBytes()));

        return headers;
    }
}
