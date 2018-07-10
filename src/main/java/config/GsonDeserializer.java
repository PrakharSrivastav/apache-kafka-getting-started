package config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    private static Logger logger = LoggerFactory.getLogger(GsonSerializer.class);
    public static final String CONFIG_VALUE_CLASS = "value.deserializer.class";
    public static final String CONFIG_KEY_CLASS = "key.deserializer.class";
    private Class<T> cls;

    private Gson gson = new GsonBuilder().create();

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        String configKey = isKey ? CONFIG_KEY_CLASS : CONFIG_VALUE_CLASS;
        String clsName = String.valueOf(config.get(configKey));
        logger.info("Loading class {}", clsName);
        try {
            cls = (Class<T>) Class.forName(clsName);
        } catch (ClassNotFoundException e) {
            logger.error("Failed to configure GsonDeserializer. " + "Did you forget to specify the '%s' property ?%n", configKey);
            logger.error("Exception is", e);
        }
    }


    @Override
    public T deserialize(String topic, byte[] bytes) {
        try {
            return (T) gson.fromJson(new String(bytes), cls);
        } catch (Exception e) {
            logger.error("Error deserializing message content ", e);
            return null;
        }
    }

    @Override
    public void close() {}
}