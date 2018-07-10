import beans.CustomProducer;
import beans.InvoiceConsumer;
import config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Application {

    static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        logger.info("Starting Application");
        // load configurations
        final AppConfig appConfig = AppConfig.load();
        System.out.println(appConfig.kafkaConfig().contentType());


        // Bootstrap Producer and send messages every minute

        CustomProducer.sendMessages(appConfig.kafkaConfig());


        // Bootstrap Producers and consumers
        InvoiceConsumer.consumeInvoiceMessages(appConfig.kafkaConfig());

    }


}
