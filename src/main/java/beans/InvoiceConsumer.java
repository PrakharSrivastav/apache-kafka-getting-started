package beans;

import config.AppConfig;
import config.GsonDeserializer;
import model.Invoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public final class InvoiceConsumer {
    private final KafkaConsumer<String, Invoice> kafkaConsumer;
    private static Logger logger = LoggerFactory.getLogger(InvoiceConsumer.class);

    public InvoiceConsumer(final AppConfig.KafkaConfig invoiceConfig) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, invoiceConfig.destination());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "c-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "ApacheKafkaGettingStarted");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class);
        properties.put("value.deserializer", GsonDeserializer.class.getName());
        properties.put(GsonDeserializer.CONFIG_VALUE_CLASS, Invoice.class.getName());
        this.kafkaConsumer = new KafkaConsumer<String, Invoice>(properties);
    }

    public KafkaConsumer<String, Invoice> getKafkaConsumer() { return kafkaConsumer; }

    public static void consumeInvoiceMessages(final AppConfig.KafkaConfig invconfig) {
//        final KafkaConsumer<String, Invoice> consumer = new InvoiceConsumer(invconfig).getKafkaConsumer();
        final AppConfig.KafkaConfig.Topic topic = invconfig.topics().get(0);
        logger.info("Starting subscription");
        try (KafkaConsumer<String, Invoice> consumer = new InvoiceConsumer(invconfig).getKafkaConsumer()) {


            consumer.subscribe(Arrays.asList(topic.name()));
            TopicPartition topicPartition = new TopicPartition("NEW_INVOICE", 0);
            List<TopicPartition> partitionList = new ArrayList<TopicPartition>();
            partitionList.add(topicPartition);
            ConsumerRecords<String, Invoice> records = consumer.poll(10000);


            consumer.seekToBeginning(partitionList);
            logger.info("Number of records found {}", records.isEmpty());

            for (ConsumerRecord<String, Invoice> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        } catch (Exception e) {
            logger.error("Error reading kafka ", e);
        }
    }


}