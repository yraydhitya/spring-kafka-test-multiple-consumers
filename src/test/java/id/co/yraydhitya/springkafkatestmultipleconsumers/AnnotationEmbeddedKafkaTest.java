package id.co.yraydhitya.springkafkatestmultipleconsumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(topics = { "topic" })
public class AnnotationEmbeddedKafkaTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    public void annotationEmbeddedKafkaTest() {
        Map<String, Object> consumerProps1 = KafkaTestUtils.consumerProps("testEmbedded", "false", broker);
        Consumer<String, String> consumer1 = new KafkaConsumer<>(consumerProps1);
        broker.consumeFromAnEmbeddedTopic(consumer1, "topic");
        System.out.println("consumer1 assignments=" + consumer1.assignment());

        Map<String, Object> consumerProps2 = KafkaTestUtils.consumerProps("testEmbedded", "false", broker);
        Consumer<String, String> consumer2 = new KafkaConsumer<>(consumerProps2);
        broker.consumeFromAnEmbeddedTopic(consumer2, "topic");
        System.out.println("consumer2 assignments=" + consumer2.assignment());
    }
}
