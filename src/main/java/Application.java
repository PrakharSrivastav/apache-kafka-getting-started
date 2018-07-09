import beans.CustomProducer;
import config.AppConfig;
import model.Invoice;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public final class Application {

    static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        logger.info("Starting Application");
        // load configurations
        final AppConfig appConfig = AppConfig.load();
        System.out.println(appConfig.kafkaConfig().contentType());

        // Bootstrap Producers and consumers


        // Bootstrap Producer and send messages every minute
        final Producer<String, Invoice> producer = new CustomProducer(appConfig.kafkaConfig()).getKafkaProducer();
        final AppConfig.KafkaConfig.Topic invoiceTopic = appConfig.kafkaConfig().topics().get(0);
        logger.info("name {}", invoiceTopic.name());

        producer.initTransactions();
        while (true) {
            try {

                producer.beginTransaction();
                Invoice invoice = Invoice.newRandomInvoice();
                logger.info("Started Producing New Invoice :: {}", invoice.toString());
                producer.send(new ProducerRecord<>(
                        invoiceTopic.name(),
                        null,
                        invoice.getInvoiceId(),
                        invoice,
                        getProducerHeaders(invoiceTopic, appConfig.kafkaConfig()))
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
    }


    private static Headers getProducerHeaders(final AppConfig.KafkaConfig.Topic topic, final AppConfig.KafkaConfig config) {
        final Headers headers = new RecordHeaders();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(topic.target());
        } catch (IOException e) {
            e.printStackTrace();
        }

        headers.add(new RecordHeader("Content-Type", config.contentType().getBytes()));
        headers.add(new RecordHeader("Encoding", config.encoding().getBytes()));
        headers.add(new RecordHeader("Source", topic.source().getBytes()));
        headers.add(new RecordHeader("Target", bos.toByteArray()));

        return headers;
    }
}
