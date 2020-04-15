package edu.nwmissouri.burke;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.Twitter;
import twitter4j.TwitterException;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.io.IOException;

public class Producer {
    /**
     * Custom Producer using Kafka for messaging. Reads properties from the
     * run.properties file in src/main/resources.
     */

    private static Scanner in;

    public static void main(String[] args) throws IOException
    {
        if (args.length != 1) 
        {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        String topicName = args[0];

        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        for (int i = 1; i <= 10; i++) 
        {
            String message = createSentence();
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, message);
            producer.send(rec);
        }


        String line = in.nextLine();
        while(!line.equals("exit")) 
        {
            //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,line);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
        
    }

    private static String createSentence()
    {
        return "Noun verb subject.";
    }

    public List<String> getTimeLine() throws TwitterException 
    {
        Twitter twitter = getTwitterinstance();
        
        return twitter.getHomeTimeline().stream().map(item -> item.getText()).collect(Collectors.toList());
    }

    private Twitter getTwitterinstance() 
    {
        return null;
    }
   
}
