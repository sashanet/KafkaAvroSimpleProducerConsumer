import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by oburyk on 4/28/2017.
 */
public class AllTest {
    private SimpleAvroProducer simpleAvroProducer;
    private Consumer simpleAvroConsumer;
    DatumReader<GenericData> payloadReader;
    private static Schema schema;
    private static GenericData.Record avroRecord;
    byte[] sendArray;
    Logger logger = Logger.getLogger(SimpleAvroProducer.class.getName());
    String message = "{\"str1\": \"Str 1-0\", \"str2\": \"Str 2-0\", \"int1\": 0, \"agentType\": \"aaaaaaa sasha0\"}";


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

    @Before
    public void init() {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(USER_SCHEMA);
        simpleAvroConsumer = new Consumer();
        simpleAvroProducer = new SimpleAvroProducer();
        payloadReader = new SpecificDatumReader<>(schema);
        avroRecord = new GenericData.Record(schema);
        avroRecord.put("str1", "Str 1-" + 1);
        avroRecord.put("str2", "Str 2-" + 1);
        avroRecord.put("int1", 1);
        avroRecord.put("agentType", "aaaaaaa sasha" + 1);
        logger.info(avroRecord.toString());
        logger.info(schema.toString());
        sendArray = new byte[]{14, 83, 116, 114, 32, 49, 45, 49, 14, 83, 116, 114, 32, 50, 45, 49, 2, 0, 28, 97, 97, 97, 97, 97, 97, 97, 32, 115, 97, 115, 104, 97, 49};
    }


    // producer Tests
    @Test
    public void calls() {
        assertEquals(0, simpleAvroProducer.getCalls());
        simpleAvroProducer.getBytes(schema, avroRecord);
        assertEquals(0, simpleAvroProducer.getCalls());
    }

    @Test
    public void compareAvroMessage() {
        assertEquals(avroRecord, simpleAvroProducer.createAvroMessage(simpleAvroProducer.getSchema(), 1));
    }

    @Test
    public void getBytes() throws IOException {
        for (int i = 0; i < sendArray.length; i++) {
            assertEquals(sendArray[i], simpleAvroProducer.getBytes(simpleAvroProducer.getSchema(), simpleAvroProducer.createAvroMessage(simpleAvroProducer.getSchema(), 1))[i]);
        }
    }

    @Test
    public void sendtoKafka() throws IOException, InterruptedException {
        assertTrue(true == simpleAvroProducer.createProducer());
    }

    // Consumer Tests
    @Test
    public void veriphyScheme() {
        assertTrue(USER_SCHEMA.equalsIgnoreCase(simpleAvroConsumer.USER_SCHEMA));
    }

/*
   @Test
    public void veriphyMessages() {
        simpleAvroProducer.createProducer();
        List<String > messages = simpleAvroConsumer.getMessages();
        if(! messages.isEmpty()) {
           assertTrue(message.equalsIgnoreCase(messages.get(0)));
       } else{
            assertTrue(false);
        }
    }*/


/*    @Test
    public void veriphyReading(){
        simpleAvroProducer.createProducer();
        List<String > messages = simpleAvroConsumer.getMessages();
        assertTrue(messages.size()!=0);
     }*/
}
