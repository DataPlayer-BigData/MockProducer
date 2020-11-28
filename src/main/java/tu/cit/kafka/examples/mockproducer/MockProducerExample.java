package tu.cit.kafka.examples.mockproducer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MockProducerExample {
//    private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "mockProducerTopic1";
        MockProducer<String,String> mockProducer = new MockProducer<String,String>(true, new DefaultPartitioner(),new StringSerializer(),new StringSerializer());

        String KEY="";
        for(int i=1;i<=10;i++) {
            KEY= i%2 == 0 ? "CS" : "IT";
            ProducerRecord<String, String> record = new ProducerRecord<>("mockProducerTopic1", KEY, "value"+i);
            Future<RecordMetadata> metadata = mockProducer.send(record);
            System.out.println("Topic : " + metadata.get().topic() + ", Partition : " + metadata.get().partition() + ", Offset : " + metadata.get().offset() + ", TimeStamp : " + metadata.get().hasTimestamp());
            //System.out.println("Topic : " + metadata.topic() + ", Partition : " + metadata.partition() + ", Offset : " + metadata.offset() + ", TimeStamp : " + metadata.timestamp());

        }

        System.out.println("Topic Partitions Details : " + mockProducer.partitionsFor("mockProducerTopic1"));
        for(int i=0;i<mockProducer.history().size();i++)
        {
            System.out.println("Topic : "+ mockProducer.history().get(i).topic()+ ", Key : " + mockProducer.history().get(i).key()
            + ", Value : "+mockProducer.history().get(i).value() );
        }

        mockProducer.clear();
        mockProducer.close();
    }

}
