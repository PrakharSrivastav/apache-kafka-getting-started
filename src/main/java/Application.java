import beans.CustomProducer;
import beans.InvoiceConsumer;
import config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Application {

    static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {

        // load configurations
        final AppConfig appConfig = AppConfig.load();

        // Bootstrap Producer and Consumer
        CustomProducer.init(appConfig.kafkaConfig());
        InvoiceConsumer.init(appConfig.kafkaConfig());

        // Start new threads for producer and consumer
        new Thread(() -> { while (true) { CustomProducer.sendMessages(appConfig.kafkaConfig()); } }).start();
        new Thread(() -> { while (true) { InvoiceConsumer.consumeMessages(); } }).start();
    }


}
