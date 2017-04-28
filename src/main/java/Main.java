/**
 * Created by oburyk on 4/28/2017.
 */
public class Main {
    public static void main(String args[]){
        SimpleAvroProducer simpleAvroProducer = new SimpleAvroProducer();
            simpleAvroProducer.createProducer();

            SimpleAvroConsumer simpleAvroConsumer = new SimpleAvroConsumer();
            simpleAvroConsumer.createConsumer();

    }
}
