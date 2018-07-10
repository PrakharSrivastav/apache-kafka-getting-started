package config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class AppConfig {

    private final KafkaConfig kafkaConfig;

    public AppConfig(KafkaConfig kafkaConfig) {this.kafkaConfig = kafkaConfig;}

    public KafkaConfig kafkaConfig() {return this.kafkaConfig;}

    public static AppConfig load() {
        final Config config = ConfigFactory.load();

        // set all the topics
        final KafkaConfig.Topic invoice = new KafkaConfig.Topic(
                config.getString("kafka.topic.invoice.name"),
                config.getString("kafka.topic.invoice.source"),
                config.getString("kafka.topic.invoice.target"),
                config.getString("kafka.topic.invoice.status"),
                config.getString("kafka.topic.invoice.action")
        );

        final KafkaConfig.Topic settle = new KafkaConfig.Topic(
                config.getString("kafka.topic.settle.name"),
                config.getString("kafka.topic.settle.source"),
                config.getString("kafka.topic.settle.target"),
                config.getString("kafka.topic.settle.status"),
                config.getString("kafka.topic.settle.action")
        );

        final KafkaConfig.Topic notification = new KafkaConfig.Topic(
                config.getString("kafka.topic.notification.name"),
                config.getString("kafka.topic.notification.source"),
                config.getString("kafka.topic.notification.target"),
                config.getString("kafka.topic.notification.status"),
                config.getString("kafka.topic.notification.action")
        );

        // set kafka config
        final String destination = config.getString("kafka.destination");
        final String contentType = config.getString("kafka.content-type");
        final String encoding = config.getString("kafka.encoding");
        final List<KafkaConfig.Topic> topics = new ArrayList<>();
        topics.add(invoice);
        topics.add(settle);
        topics.add(notification);

        final KafkaConfig kafkaConfig = new KafkaConfig(destination,contentType,encoding,topics);

        // set application config
        return new AppConfig(kafkaConfig);
    }

    public static class KafkaConfig {
        private final String destination;
        private final String contentType;
        private final String encoding;
        private final List<Topic> topics;

        public KafkaConfig(String destination, String contentType, String encoding, List<Topic> topics) {
            this.destination = destination;
            this.contentType = contentType;
            this.encoding = encoding;
            this.topics = topics;
        }

        public static class Topic {
            private final String name;
            private final String source;
            private final String target;
            private final String status;
            private final String action;

            public Topic(String name, String source, String target, String status, String action) {
                this.name = name;
                this.source = source;
                this.target = target;
                this.status = status;
                this.action = action;
            }

            public String name() { return name; }

            public String source() { return source; }

            public String target() { return target; }

            public String status() { return status; }

            public String action() { return action; }
        }

        public String destination() { return destination; }

        public String contentType() { return contentType; }

        public String encoding() { return encoding; }

        public List<Topic> topics() { return topics; }
    }
}
