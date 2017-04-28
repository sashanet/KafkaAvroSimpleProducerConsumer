import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer {

   private List<String> messages = new ArrayList<>();
 /*   public static void main(String[] args) {
          createConsumer();
    }*/

    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + " \"namespace\": \"com.example\","
            + "\"name\":\"message\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\", \"default\": null  },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" , \"default\": 0},"
            + "   {\"name\": \"agentType\",  \"type\": [\"string\",\"null\"], \"default\": \"APP_AGENT\"  }"
            + "]}";


    public static final String USER_SCHEMA2 = "{"
            + "\"type\":\"record\","
            + " \"namespace\": \"com.example\","
            + "\"name\":\"message\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\", \"default\": null  },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\", \"default\": 0},"
            + "   {\"name\": \"agentType\",  \"type\": [\"string\",\"null\"], \"default\": \"APP_AGENT\"  }"
            + "]}";

    public  void createConsumer() {
        Properties props = new Properties();
        createPropsToConsumer(props);
        final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer(props);
        consumer.listTopics().keySet().forEach(str -> System.out.println("Topic = " + str));
        consumer.subscribe(Arrays.asList("lasttopic"));
        Schema.Parser parser = new Schema.Parser();
        Schema.Parser parser2 = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        Schema schema2 = parser2.parse(USER_SCHEMA2);

        DatumReader<GenericData> payloadReader = new SpecificDatumReader<>(schema, schema2);
        while (messages.isEmpty()) {
            //        consumer.poll(100).forEach(consumer1 -> System.out.printf(" \n value = %s , From Topic = %s ", consumer1.value(), consumer1.topic()));
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            for (ConsumerRecord<String, byte[]> record : records) {
                Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
                try {
                    String message = String.valueOf(payloadReader.read(null, decoder));
                    messages.add(message);


                    System.out.println(message);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public List<String> getMessages() {
        createConsumer();
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    private static void parseSecondScheme(DatumReader<GenericRecord> payloadReader1, Decoder decoder2) {
        try {
            System.out.println("aaa = " + payloadReader1.read(null, decoder2));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private static void createPropsToConsumer(Properties props) {
        props.put("bootstrap.servers", "localhost:9092");
        //      props.put("auto.offset.reset", "earliest");
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

  /*  private static void createProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
    *//*    props.put("partitioner.class", "SimplePartitioner");
        props.put("request.required.acks", "1");*//*
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        KeyedMessage<String, String> data;
        for (int i = 0; i < 10; i++) {
            data = new KeyedMessage<String, String>("test1", "message " + i);
            producer.send(data);
        }
        producer.close();
    }
*/
}

//Serialization In File
/*    File file = new File("users.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
                dataFileWriter.append(avroRecord);
                dataFileWriter.close();
//Decerealization in file
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
        user = dataFileReader.next(user);
        System.out.println(user);
        }*/




    /* for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
            producer.send(data);
        }*/