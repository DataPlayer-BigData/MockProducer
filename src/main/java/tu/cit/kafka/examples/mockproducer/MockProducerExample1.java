package tu.cit.kafka.examples.mockproducer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.java2d.pipe.SpanShapeRenderer;

import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;

public class MockProducerExample1 {
    //private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "mockProducerTopic1";

        Node node0 =new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);
        Node node2 = new Node(2, "localhost", 9094);

        Node[] replica0 = new Node[]{node1,node2};
        Node[] replica1 = new Node[]{node2,node0,node1} ;
        Node[] replica2 = new Node[]{node1,node0,node2} ;

        List<Node> nodes = new ArrayList<>();
        nodes.add(node0);
        nodes.add(node1);
        nodes.add(node2);

        PartitionInfo partitionInfo0 = new PartitionInfo(topicName,0,node1,replica0 ,replica0,new Node[]{node0});
        PartitionInfo partitionInfo1 = new PartitionInfo(topicName,1,node2,replica1,replica1,new Node[]{node0});
        PartitionInfo partitionInfo2 = new PartitionInfo(topicName,2,node1,replica2,replica2,new Node[]{node0});

        Cluster cluster = new Cluster(null,nodes,asList(partitionInfo0,partitionInfo1,partitionInfo2), Collections.<String>emptySet(), Collections.<String>emptySet());
        //Cluster cluster = new Cluster("kafkab", new ArrayList<Node>(), list, emptySet(), emptySet());

        MockProducer<String,String> mockProducer = new MockProducer<String,String>(cluster,true, new RoundRobinPartitioner(),new StringSerializer(),new StringSerializer());
        //MockProducer<String,String> mockProducer = new MockProducer<String,String>(true, new DefaultPartitioner(),new StringSerializer(),new StringSerializer());
        //MockProducer<String,String> mockProducer = new MockProducer<String,String>(true, new RoundRobinPartitioner(),new StringSerializer(),new StringSerializer());
        String KEY="";
        Long msgDate = System.currentTimeMillis();
        for(int i=1;i<=20;i++) {
            KEY= i%2 == 0 ? "CS" : "IT";
            //If you want to sent the Create Time manually
            //ProducerRecord<String, String> record = new ProducerRecord<String,String>("mockProducerTopic1",1,msgDate, KEY, "value"+i);

            ProducerRecord<String, String> record = new ProducerRecord<String,String>("mockProducerTopic1", KEY, "value"+i);

            Future<RecordMetadata> metadata = mockProducer.send(record);
            System.out.println("Topic : " + metadata.get().topic() + ", Partition : " + metadata.get().partition() + ", Offset : " + metadata.get().offset() + ", TimeStamp : " + metadata.get().timestamp());

            //System.out.println("Topic : " + metadata.topic() + ", Partition : " + metadata.partition() + ", Offset : " + metadata.offset() + ", TimeStamp : " + metadata.timestamp());
        }

        System.out.println(mockProducer.partitionsFor("mockProducerTopic1"));

        for(int i=0;i<mockProducer.history().size();i++)
        {
            SimpleDateFormat  simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd HH:MM:SS SSS Z");
            Date dt = new Date(mockProducer.history().get(i).timestamp() == null? System.currentTimeMillis():System.currentTimeMillis());
            String mDate  = simpleDateFormat.format(dt);
            System.out.println("Topic : "+ mockProducer.history().get(i).topic()+", TimeStamp : " + mDate + ", Key : " + mockProducer.history().get(i).key()
            + ", Value : "+mockProducer.history().get(i).value() );
        }

        mockProducer.clear();
        mockProducer.close();
    }

}
