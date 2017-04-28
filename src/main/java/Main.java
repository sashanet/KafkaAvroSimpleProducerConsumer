import java.util.Scanner;

/**
 * Created by oburyk on 4/28/2017.
 */


public class Main {
    public static void main(String args[]) {


        System.out.println("1 - produce");
        System.out.println("2 - consume");
        Scanner scanner = new Scanner(System.in);

        while (true) {
            String str = scanner.nextLine();
            switch (str) {
                case "1":
                    produce();
                    break;
                case "2":
                    consume();
                    break;
                default:
                    System.out.println("wrong");
                    break;
            }
        }
    }

    public static void consume() {
        Consumer simpleAvroConsumer = new Consumer();
        simpleAvroConsumer.createConsumer();
    }

    public static void produce() {
        SimpleAvroProducer simpleAvroProducer = new SimpleAvroProducer();
        simpleAvroProducer.createProducer();
    }
}
