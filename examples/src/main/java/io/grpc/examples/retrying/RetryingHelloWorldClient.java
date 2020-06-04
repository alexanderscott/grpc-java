/*
 * Copyright 2019 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.examples.retrying;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;

import java.io.InputStreamReader;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A client that requests a greeting from the {@link RetryingHelloWorldServer} with a retrying policy.
 */
public class RetryingHelloWorldClient {
  static final String ENV_ENABLE_RETRY_FUTURES = "ENV_ENABLE_RETRY_FUTURES_IN_EXAMPLE";

  private static final Logger logger = Logger.getLogger(RetryingHelloWorldClient.class.getName());

  private final CountDownLatch countdownLatch;
  private final boolean retryingFutures;
  private final ManagedChannel channel;
  private final GreeterGrpc.GreeterBlockingStub blockingStub;
  private final GreeterGrpc.GreeterFutureStub futureStub;
  private final PriorityBlockingQueue<Long> latencies = new PriorityBlockingQueue<>();
  private final AtomicInteger failedRpcs = new AtomicInteger();

  /**
   * Construct client connecting to HelloWorld server at {@code host:port}.
   */
  public RetryingHelloWorldClient(String host, int port, int callCount, boolean enableFutures) {
    Map<String, ?> retryingServiceConfig =
        new Gson()
            .fromJson(
                new JsonReader(
                    new InputStreamReader(
                        RetryingHelloWorldClient.class.getResourceAsStream(
                            "retrying_service_config.json"),
                        UTF_8)),
                Map.class);

    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port)
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        .usePlaintext();
    channelBuilder.defaultServiceConfig(retryingServiceConfig).enableRetry();
    channel = channelBuilder.build();
    blockingStub = GreeterGrpc.newBlockingStub(channel);
    futureStub = GreeterGrpc.newFutureStub(channel);
    this.retryingFutures = enableFutures;
    this.countdownLatch = new CountDownLatch(callCount);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(30, TimeUnit.SECONDS);
  }

  public ListenableFuture<HelloReply> greetFuture(String name) {
    HelloRequest request = HelloRequest.newBuilder().setName(name).build();
    HelloReply response = null;
    final long startTime = System.nanoTime();
    final String currentName = name;
    logger.log(Level.INFO, "Starting request for user {0}", new Object[]{name});
    ListenableFuture<HelloReply> responseFuture = futureStub.sayHello(request);
    Futures.addCallback(responseFuture, new FutureCallback<HelloReply>() {
      @Override
      public void onSuccess(HelloReply response) {
        logger.log(Level.INFO, "Successful RPC response for user {0}: {1}", new Object[]{currentName, response.getMessage()});
        long latencyMills = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        latencies.offer(latencyMills);
        countdownLatch.countDown();
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
          failedRpcs.incrementAndGet();
          StatusRuntimeException e = (StatusRuntimeException) throwable;
          logger.log(Level.INFO, "Failure RPC response for user {0}: {1}",
              new Object[]{currentName, e});
          long latencyMills = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
          latencies.offer(latencyMills);
          countdownLatch.countDown();
        }
      }
    }, MoreExecutors.directExecutor());

    return responseFuture;
  }

  /**
   * Say hello to server.
   */
  public void greet(String name) {
    HelloRequest request = HelloRequest.newBuilder().setName(name).build();
    HelloReply response = null;
    StatusRuntimeException statusRuntimeException = null;
    long startTime = System.nanoTime();
    try {
      response = blockingStub.sayHello(request);
    } catch (StatusRuntimeException e) {
      failedRpcs.incrementAndGet();
      statusRuntimeException = e;
    }
    long latencyMills = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    latencies.offer(latencyMills);

    if (statusRuntimeException == null) {
      logger.log(
          Level.INFO,
          "Greeting: {0}. Latency: {1}ms",
          new Object[]{response.getMessage(), latencyMills});
    } else {
      logger.log(
          Level.INFO,
          "RPC failed: {0}. Latency: {1}ms",
          new Object[]{statusRuntimeException.getStatus(), latencyMills});
    }
    countdownLatch.countDown();
  }

  void printSummary() {
    try {
      countdownLatch.await();
    } catch(InterruptedException e) {
      logger.log(Level.INFO, "Countdown latch interrupted, exiting");
      return;
    }
    int rpcCount = latencies.size();
    long latency50 = 0L;
    long latency90 = 0L;
    long latency95 = 0L;
    long latency99 = 0L;
    long latency999 = 0L;
    long latencyMax = 0L;
    for (int i = 0; i < rpcCount; i++) {
      long latency = latencies.poll();
      if (i == rpcCount * 50 / 100 - 1) {
        latency50 = latency;
      }
      if (i == rpcCount * 90 / 100 - 1) {
        latency90 = latency;
      }
      if (i == rpcCount * 95 / 100 - 1) {
        latency95 = latency;
      }
      if (i == rpcCount * 99 / 100 - 1) {
        latency99 = latency;
      }
      if (i == rpcCount * 999 / 1000 - 1) {
        latency999 = latency;
      }
      if (i == rpcCount - 1) {
        latencyMax = latency;
      }
    }

    logger.log(
        Level.INFO,
        "\n\nTotal RPCs sent: {0}. Total RPCs failed: {1}\n"
            + "[Retrying enabled]\n"
            + "========================\n"
            + "50% latency: {2}ms\n"
            + "90% latency: {3}ms\n"
            + "95% latency: {4}ms\n"
            + "99% latency: {5}ms\n"
            + "99.9% latency: {6}ms\n"
            + "Max latency: {7}ms\n"
            + "========================\n",
        new Object[]{
            rpcCount, failedRpcs.get(),
            latency50, latency90, latency95, latency99, latency999, latencyMax});

    if (retryingFutures) {
      logger.log(
          Level.INFO,
          "Testing with GRPC future stubs.  To test with blocking stubs, run the client with environment variable {0}=false.",
          ENV_ENABLE_RETRY_FUTURES);
    } else {
      logger.log(
          Level.INFO,
          "Testing with GRPC blocking stubs.  To test with future stubs, run the client with environment variable {0}=true.",
          ENV_ENABLE_RETRY_FUTURES);
    }
  }

  public static void main(String[] args) throws Exception {
//    final boolean retryingFutures = Boolean.parseBoolean(System.getenv(ENV_ENABLE_RETRY_FUTURES));
    final boolean retryingFutures = true;
    final int callCount = 20;

    final RetryingHelloWorldClient client = new RetryingHelloWorldClient("localhost", 50051, callCount, retryingFutures);
    final ForkJoinPool executor = new ForkJoinPool();
    final Queue<ListenableFuture<HelloReply>> greetFutures = new ConcurrentLinkedQueue<ListenableFuture<HelloReply>>();
    for (int i = 0; i < callCount; i++) {
      final String userId = "user" + i;
      executor.execute(
          new Runnable() {
            @Override
            public void run() {
              if (retryingFutures) {
                greetFutures.add(client.greetFuture(userId));
              } else {
                client.greet(userId);
              }
            }
          });
    }

    executor.awaitQuiescence(100, TimeUnit.SECONDS);
    logger.log(Level.INFO, "Completed all GRPC calls");
    executor.shutdown();
    client.printSummary();
    client.shutdown();
  }
}
