package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe{


	public static void main(String[] args)throws Exception{

        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-pipe");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.RETRIES_CONFIG,3);
        properties.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG,100);
        final StreamsBuilder builder=new StreamsBuilder();
        KStream<String,String> source=builder.stream("streams-plaintext-input2");
        source.to("streams-pipe-output");
        final Topology topology=builder.build();
            System.out.println(topology.describe());
            final KafkaStreams streams=new KafkaStreams(topology,properties);
            final CountDownLatch countDownLatch=new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){

                    @Override
                    public void run() {
                            super.run();
                            streams.close();
                            countDownLatch.countDown();

                    }
            });

            try {
                    streams.start();
                    countDownLatch.await();
            }catch (Exception e){
                    e.printStackTrace();
                    System.exit(1);
            }
                System.exit(0);




}

}
