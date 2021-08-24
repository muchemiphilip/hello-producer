package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;

public class HelloProducer {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID);
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,AppConfigs.transaction_id);

        logger.info("Start sending message....");

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(properties);
        producer.initTransactions();

        producer.beginTransaction();
        try {
            for (int i = 0; i < AppConfigs.numEvents; i++){
                producer.send(new ProducerRecord<>(AppConfigs.topicName1,i,"Just a simple message-T1 " + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2,i,"Just a simple message-T1" + i));
            }
            logger.info("Committing first transaction.");
            producer.commitTransaction();
        } catch (Exception e) {
            logger.error("Error in first transaction. Aborting..... ");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        try {
            for (int i = 0; i < AppConfigs.numEvents; i++){
                producer.send(new ProducerRecord<>(AppConfigs.topicName1,i,"Just a simple message-T2 " + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2,i,"Just a simple message-T2" + i));
            }
            logger.info("Committing Second transaction.");
            producer.abortTransaction();
        } catch (Exception e) {
            logger.error("Error in Second transaction. Aborting..... ");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("Finished sending info, the producer is closing.");
        producer.flush();
    }
}
