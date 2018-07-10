package beans;

import config.AppConfig;
import config.GsonDeserializer;
import model.Invoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public final class InvoiceConsumer {
    private static KafkaConsumer<String, Invoice> kafkaConsumer;
    private static Logger logger = LoggerFactory.getLogger(InvoiceConsumer.class);
    private static Boolean consumerExists = false;

    public static void init(final AppConfig.KafkaConfig config) {

        if (!consumerExists) {
            logger.info("Instantiating a new Consumer");
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.destination());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "c-group");
            // properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "ApacheKafkaGettingStarted");
            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class);
            properties.put(GsonDeserializer.CONFIG_VALUE_CLASS, Invoice.class.getName());
            kafkaConsumer = new KafkaConsumer<>(properties);
            consumerExists = true;
        } else {
            kafkaConsumer = getKafkaConsumer();
        }

        // subscribe to topic
        getKafkaConsumer().subscribe(Arrays.asList(config.topics().get(0).name()));
    }

    private static KafkaConsumer<String, Invoice> getKafkaConsumer() { return kafkaConsumer; }

    public static void consumeMessages() {
        try {
            for (ConsumerRecord<String, Invoice> record : getKafkaConsumer().poll(0)) {
                logger.info("Consuming {}", record.value());
            }
        } catch (Exception e) {
            logger.error("Error reading kafka ", e);
        }
    }


}
