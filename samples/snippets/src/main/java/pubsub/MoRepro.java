package pubsub;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.Handler;
import java.time.LocalDateTime;
import java.lang.Thread;
import org.threeten.bp.Duration;
import java.util.concurrent.Future;

public class MoRepro {
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
        System.out.println(Thread.currentThread().getName() + "; Message ID: " + message.getMessageId() + "; Received at " + now + "; Data: " + message.getData().toStringUtf8() + ";" + sleepFor);
      } catch (Exception e) {
        System.err.println("!!!!error - " + message.getData().toStringUtf8());
        e.printStackTrace();
      }
    };

    // MessageReceiverWithAckResponse receiverWithResponse =
    //     (PubsubMessage message, AckReplyConsumerWithResponse consumerWithResponse) -> {
    //       try {
    //         // Handle incoming message, then ack the message, and receive an ack response.
    //         long sleepFor = (int) (Math.random() * 200);
    //         Thread.sleep(sleepFor);
    //         LocalDateTime now = LocalDateTime.now();
    //         System.out.println(Thread.currentThread().getName() + "; Message ID: " + message.getMessageId() + "; Received at " + now + "; Data: " + message.getData().toStringUtf8() + ";" + sleepFor);
    //         Future<AckResponse> ackResponseFuture = consumerWithResponse.ack();

    //         // Retrieve the completed future for the ack response from the server.
    //         AckResponse ackResponse = ackResponseFuture.get();

    //         switch (ackResponse) {
    //           case SUCCESSFUL:
    //             // Success code means that this MessageID will not be delivered again.
    //             System.out.println("Message successfully acked: " + message.getMessageId());
    //             break;
    //           case INVALID:
    //             System.err.println(
    //                 "Message failed to ack with a response of Invalid. Id: "
    //                     + message.getMessageId());
    //             break;
    //           case PERMISSION_DENIED:
    //             System.err.println(
    //                 "Message failed to ack with a response of Permission Denied. Id: "
    //                     + message.getMessageId());
    //             break;
    //           case FAILED_PRECONDITION:
    //             System.err.println(
    //                 "Message failed to ack with a response of Failed Precondition. Id: "
    //                     + message.getMessageId());
    //             break;
    //           case OTHER:
    //             System.err.println(
    //                 "Message failed to ack with a response of Other. Id: "
    //                     + message.getMessageId());
    //             break;
    //           default:
    //             break;
    //         }
    //       } catch (InterruptedException | ExecutionException e) {
    //         System.err.println(
    //             "MessageId: " + message.getMessageId() + " failed when retrieving future");
    //       } catch (Throwable t) {
    //         System.err.println("Throwable caught" + t.getMessage());
    //       }
    //     };
    
    Subscriber subscriber = Subscriber.newBuilder(subscriptionName, messageReceiver)
      .setFlowControlSettings(FlowControlSettings.newBuilder().setMaxOutstandingElementCount(1000L).setMaxOutstandingRequestBytes(104857600L).build())
      .setMaxAckExtensionPeriod(Duration.ofMinutes(60L))
      .setMaxDurationPerAckExtension(Duration.ofSeconds(0L))
      .setParallelPullCount(2)
      .build();
      // .setMinDurationPerAckExtension(Duration.ofSeconds(60L))
    
    System.err.println("Starting subscriber");
    subscriber.startAsync().awaitRunning();
    
    try {
      LocalDateTime now = LocalDateTime.now();
      for (int i = 0; i <= 500; i++) {
        String data = "prefix," + now + "," + i;
        
        try {
          long sleepFor = (int) (Math.random() * 200);
          System.out.println(Thread.currentThread().getName() + ";Sending;" + data + ";" + sleepFor);
          Thread.sleep(sleepFor);
        } catch (Exception e) {
          e.printStackTrace();
        }
        
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
      subscriber.awaitTerminated(150, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      subscriber.stopAsync();
    }
  }    
}