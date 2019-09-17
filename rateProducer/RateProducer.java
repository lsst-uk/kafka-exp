import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RateProducer {

   public static void main(String[] args) throws Exception{
	   
      int delay = 100000; // 0.1ms
      //int delay = 500000000; // 0.5s

      if(args.length != 3){
         System.out.println("Usage: java RateProducer <duration> <rate> <topic>");
         return;
      }

      long duration = (long) Double.parseDouble(args[0]) * 1000000000;
      int rate = Integer.parseInt(args[1]);
      String topicName = args[2].toString();

      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("batch.size", 16384);
      props.put("linger.ms", 1);
      props.put("buffer.memory", 33554432);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);

      long messagesSent = 0;
      long elapsedTime;
      long startTime = System.nanoTime();
      long previousTime = startTime;
      while (true) {
         long now = System.nanoTime();
         elapsedTime = now - startTime;
	 if ((now - previousTime) > delay) {
            long nMessages = (rate * elapsedTime / 1000000000) - messagesSent;
	    //System.out.printf("Sending %d messages at %d.%n", nMessages, elapsedTime);
            for(int i = 0; i < nMessages; i++) {
	       //System.out.printf(".");
               producer.send(new ProducerRecord<String, String>(topicName,
               Integer.toString(i), Integer.toString(i)));
	       messagesSent += 1;
            }
	    producer.flush();
	    //System.out.printf("%nSent %d messages at %d.%n", messagesSent, elapsedTime);
	    previousTime = now;
	 }
	 if (elapsedTime > duration) {
            // try to ensure that we don't send less than the expected number of messages
            if (messagesSent >= (duration * rate / 1000000000)) {
	       break;
	    }
	    // but give up after an extra 0.1 second
	    if (elapsedTime > duration + 100000000) {
	       break;
	    }
	 }
      }

      double totalTime = (System.nanoTime() - startTime) / 1000000000.0;
      double actualRate = messagesSent / totalTime;

      producer.close();

      System.out.printf("Sent %d messages in %f seconds at a rate of %f messages/second.%n",
		      messagesSent, totalTime, actualRate);


   }

}
