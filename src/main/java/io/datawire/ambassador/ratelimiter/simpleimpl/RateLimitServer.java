package io.datawire.ambassador.ratelimiter.simpleimpl;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.lyft.ratelimit.RateLimitServiceGrpc;
import pb.lyft.ratelimit.Ratelimit;


public class RateLimitServer {

    private static final Logger LOGGER = LogManager.getLogger(RateLimitServer.class);

    private static final int port = 50051;

    private Server server;

    private void start() throws Exception {
        LOGGER.info("Attempting to start server listening on {}", port);

        server = ServerBuilder.forPort(port)
                .addService(new RateLimiterImpl())
                .build()
                .start();

        LOGGER.info("Server started, listening on {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("Shutting down gRPC server since JVM is shutting down");
            RateLimitServer.this.stop();
            System.err.println("gRPC Server shut down");
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws Exception {
        final RateLimitServer server = new RateLimitServer();
        server.start();
        server.blockUntilShutdown();
    }

    private static class RateLimiterImpl extends RateLimitServiceGrpc.RateLimitServiceImplBase {

        @Override
        public void shouldRateLimit(Ratelimit.RateLimitRequest rateLimitRequest, StreamObserver<Ratelimit.RateLimitResponse> responseStreamObserver) {
            LOGGER.info("Domain {}", rateLimitRequest.getDomain());
            LOGGER.info("DescriptorsCount {}", rateLimitRequest.getDescriptorsCount());
            rateLimitRequest.getDescriptorsList()
                    .forEach(d -> d.getEntriesList()
                            .forEach(e -> LOGGER.info("k: {} v: {}", e.getKey(), e.getValue())));

            Ratelimit.RateLimitResponse rateLimitResponse = Ratelimit.RateLimitResponse.newBuilder()
                    .setOverallCode(Ratelimit.RateLimitResponse.Code.OK).build();
            responseStreamObserver.onNext(rateLimitResponse);
            responseStreamObserver.onCompleted();

        }
    }
}
