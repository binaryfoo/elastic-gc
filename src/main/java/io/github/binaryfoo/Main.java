package io.github.binaryfoo;

import io.github.binaryfoo.gclog.GCEvent;
import io.github.binaryfoo.gclog.JavaParser;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Read a jvm garbage collection log and push each event to an elasticsearch index named gc.
 */
public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    Client client = TransportClient.builder().build()
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

    BulkProcessor processor = BulkProcessor.builder(client, LISTENER).build();

    String gcLog = IOUtils.toString(new GZIPInputStream(new FileInputStream(args[0])));
    List<GCEvent> gcEvents = JavaParser.parseLog(gcLog);
    for (GCEvent event : gcEvents) {
      XContentBuilder b = XContentFactory.jsonBuilder().startObject();
      for (Tuple2<String, Object> attribute : JavaConversions.asJavaIterable(event.toExport())) {
        String name = attribute._1();
        Object value = attribute._2();
        if (value instanceof DateTime) {
          b.field(name, ((DateTime) value).toDate());
        } else {
          b.field(name, value);
        }
      }
      processor.add(new IndexRequest("gc", "gc").source(b.endObject()));
    }

    processor.awaitClose(1, TimeUnit.MINUTES);
    client.close();
  }

  private static final BulkProcessor.Listener LISTENER = new BulkProcessor.Listener() {
    public void beforeBulk(long executionId, BulkRequest request) {
      System.out.println("before " + executionId);
    }

    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
      System.out.println("after " + executionId);
    }

    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
      System.err.println("after " + executionId);
      failure.printStackTrace();
    }
  };
}
