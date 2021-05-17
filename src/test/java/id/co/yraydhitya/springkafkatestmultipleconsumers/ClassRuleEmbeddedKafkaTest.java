package id.co.yraydhitya.springkafkatestmultipleconsumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ClassRuleEmbeddedKafkaTest {

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, false, "topic");

    private EmbeddedKafkaBroker broker = embeddedKafkaRule.getEmbeddedKafka();

    @Test
    public void classRuleEmbeddedKafkaTest() {
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
