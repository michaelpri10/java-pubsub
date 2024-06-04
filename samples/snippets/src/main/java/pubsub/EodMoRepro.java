package pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.AckResponse;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.lang.Thread;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.threeten.bp.Duration;

public class EodMoRepro {
  public static void main(String... args) throws Exception {
    String project = "cloud-pubsub-experiments";
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, "mike-eod-mo-sub");
    TopicName topicName = TopicName.of(project, "mike-eod-mo");
    Publisher publisher = Publisher.newBuilder(topicName).setEnableMessageOrdering(true).build();
    
    MessageReceiver messageReceiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
      try {
        long sleepFor = (int) (Math.random() * 200);
        Thread.sleep(sleepFor);
        LocalDateTime now = LocalDateTime.now();
        consumer.ack();
        System.out.println(
          Thread.currentThread().getName() +
          "; Message ID: " + message.getMessageId() +
          "; Received at " + now +
          "; Data: " + message.getData().toStringUtf8() +
          "; Slept: " + sleepFor);
      } catch (Exception e) {
        System.err.println("!!!!error - " + message.getData().toStringUtf8());
        e.printStackTrace();
      }
    };
    
    Subscriber subscriber = Subscriber.newBuilder(subscriptionName, messageReceiver)
      .setFlowControlSettings(
        FlowControlSettings.newBuilder()
          .setMaxOutstandingElementCount(1000L)
          .setMaxOutstandingRequestBytes(104857600L).build())
      .setMaxAckExtensionPeriod(Duration.ofMinutes(60L))
      .setMaxDurationPerAckExtension(Duration.ofSeconds(0L))
      .setParallelPullCount(2)
      .build();
    
    System.err.println("Starting subscriber");
    subscriber.startAsync().awaitRunning();
    
    try {
      for (int i = 0; i <= 500; i++) {
        // long sleepFor = (int) (Math.random() * 200);
        // Thread.sleep(sleepFor);
        LocalDateTime now = LocalDateTime.now();
        String data = now + "," + i;
        // System.out.println(Thread.currentThread().getName() + "; Sending data: " + data);
        
        PubsubMessage message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(data))
          .setOrderingKey("defabc")
          .build();
        publisher.publish(message);
      }
    } finally {
      publisher.shutdown();
      publisher.awaitTermination(1, TimeUnit.MINUTES);
    }
    
    try {
      System.err.println("Awaiting termination");
      subscriber.awaitTerminated(900, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      subscriber.stopAsync();
    }
  }    
}