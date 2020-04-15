package edu.nwmissouri.burke;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import java.util.List;
import java.util.Properties;
//import java.util.Scanner;
import java.util.stream.Collectors;
import java.io.IOException;

public class Producer {
    /**
     * Custom Producer using Kafka for messaging. Reads properties from the
     * run.properties file in src/main/resources.
     */

//    private static Scanner in;

    public static void main(String[] args) throws IOException, TwitterException
    {
        if (args.length != 1) 
        {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        String topicName = args[0];

//        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);


        // The factory instance is re-useable and thread safe.
        Twitter twitter = TwitterFactory.getSingleton();
        List<Status> statuses = twitter.getHomeTimeline();
        System.out.println("Showing home timeline.");
        for (Status status : statuses) {
            String tweet = status.getUser().getName() + " : " + status.getText() + "\n";
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, tweet);
            producer.send(rec);
 
        
        }   
        producer.close();     
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
