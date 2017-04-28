/**
 * Created by oburyk on 4/25/2017.
 */
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class SimpleAvroProducer {
    int calls;

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



    public  boolean createProducer()  {
        Properties props = getProperties();
        Schema schema = getSchema();

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            GenericData.Record avroRecord = createAvroMessage(schema, i);
            byte[] serializedBytes = getBytes(schema, avroRecord);
            System.out.println(avroRecord);
            System.out.println(Arrays.toString(serializedBytes));
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("lasttopic", serializedBytes);
            producer.send(record);
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
            return false;
            }
        }
        producer.close();
        return true;
    }

    public Schema getSchema() {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(USER_SCHEMA);
    }

    public GenericData.Record createAvroMessage(Schema schema, int i) {
        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("str1", "Str 1-" + i);
        avroRecord.put("str2", "Str 2-" + i);
        avroRecord.put("int1", i);
        avroRecord.put("agentType", "aaaaaaa sasha" + i);
        return avroRecord;
    }

    public  byte[] getBytes(Schema schema, GenericData.Record avroRecord)  {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericData.Record> writer = new SpecificDatumWriter<>(schema);
        try {
            writer.write(avroRecord, encoder);
              encoder.flush();
        } catch (IOException e) {
            return new byte[]{1};
        }
        try {
            out.close();
        } catch (IOException e) {
            return new byte[]{1};
        }
        return out.toByteArray();
    }

    public  Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }

    public int getCalls() {
        return calls;
    }

}