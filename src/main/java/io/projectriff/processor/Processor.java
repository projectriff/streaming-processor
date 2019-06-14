package io.projectriff.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.bsideup.liiklus.protocol.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.projectriff.invoker.rpc.*;
import io.projectriff.processor.serialization.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Main driver class for the streaming processor.
 *
 * <p>Continually pumps data from one or several input streams (see {@code riff-serialization.proto} for this so-called "at rest" format),
 * arranges messages in invocation windows and invokes the riff function over RPC by multiplexing messages from several
 * streams into one RPC channel (see {@code riff-rpc.proto} for the wire format).
 * On the way back, performs the opposite operations: de-muxes results and serializes them back to the corresponding
 * output streams.</p>
 *
 * @author Eric Bottard
 * @author Florent Biville
 */
public class Processor {

    /**
     * ENV VAR key holding the coordinates of the input streams, as a comma separated list of {@code gatewayAddress:port/streamName}.
     *
     * @see FullyQualifiedTopic
     */
    public static final String INPUTS = "INPUTS";

    /**
     * ENV VAR key holding the coordinates of the output streams, as a comma separated list of {@code gatewayAddress:port/streamName}.
     *
     * @see FullyQualifiedTopic
     */
    public static final String OUTPUTS = "OUTPUTS";

    /**
     * ENV VAR key holding the address of the function RPC, as a {@code host:port} string.
     */
    public static final String FUNCTION = "FUNCTION";

    /**
     * ENV VAR key holding the serialized list of content-types expected on the output streams.
     *
     * @see StreamOutputContentTypes
     */
    public static final String OUTPUT_CONTENT_TYPES = "OUTPUT_CONTENT_TYPES";

    /**
     * ENV VAR key holding the consumer group string this process should use.
     */
    public static final String GROUP = "GROUP";

    /** The number of retries when testing http connection to the function. */
    private static final int NUM_RETRIES = 20;

    /**
     * Keeps track of a single gRPC stub per gateway address.
     */
    private final Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> liiklusInstancesPerAddress;

    /** The ordered input streams for the function, in parsed form. */
    private final List<FullyQualifiedTopic> inputs;

    /** The ordered output streams for the function, in parsed form. */
    private final List<FullyQualifiedTopic> outputs;

    private final List<String> outputContentTypes;

    /**
     * The consumer group string this process will use to identify itself when reading from the input streams.
     */
    private final String group;

    /**
     * The RPC stub used to communicate with the function process.
     *
     * @see "riff-rpc.proto for the wire format and service definition"
     */
    private final ReactorRiffGrpc.ReactorRiffStub riffStub;

    public static void main(String[] args) throws Exception {

        checkEnvironmentVariables();

        Hooks.onOperatorDebug();

        String functionAddress = System.getenv(FUNCTION);

        assertHttpConnectivity(functionAddress);

        var fnChannel = NettyChannelBuilder.forTarget(functionAddress)
                .usePlaintext()
                .build();

        var inputAddressableTopics = FullyQualifiedTopic.parseMultiple(System.getenv(INPUTS));
        var outputAdressableTopics = FullyQualifiedTopic.parseMultiple(System.getenv(OUTPUTS));
        var processor = new Processor(
                inputAddressableTopics,
                outputAdressableTopics,
                parseContentTypes(System.getenv(OUTPUT_CONTENT_TYPES), outputAdressableTopics.size()),
                System.getenv(GROUP),
                ReactorRiffGrpc.newReactorStub(fnChannel));

        processor.run();

    }

    private static void checkEnvironmentVariables() {
        List<String> envVars = Arrays.asList(INPUTS, OUTPUTS, OUTPUT_CONTENT_TYPES, FUNCTION, GROUP);
        if (envVars.stream()
                .anyMatch(v -> (System.getenv(v) == null || System.getenv(v).trim().length() == 0))) {
            System.err.format("Missing one of the following environment variables: %s%n", envVars);
            envVars.forEach(v -> System.err.format("  %s = %s%n", v, System.getenv(v)));
            System.exit(1);
        }
    }

    private static void assertHttpConnectivity(String functionAddress) throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("http://" + functionAddress);
        for (int i = 1; i <= NUM_RETRIES; i++) {
            try (Socket s = new Socket(uri.getHost(), uri.getPort())) {
            } catch (ConnectException t) {
                if (i == NUM_RETRIES) {
                    throw t;
                }
                Thread.sleep(i * 100);
            }
        }
    }

    public Processor(List<FullyQualifiedTopic> inputs,
                     List<FullyQualifiedTopic> outputs,
                     List<String> outputContentTypes,
                     String group,
                     ReactorRiffGrpc.ReactorRiffStub riffStub) {

        this.inputs = inputs;
        this.outputs = outputs;
        var allGateways = new HashSet<>(inputs);
        allGateways.addAll(outputs);

        this.liiklusInstancesPerAddress = indexByAddress(allGateways);
        this.outputContentTypes = outputContentTypes;
        this.riffStub = riffStub;
        this.group = group;
    }

    public void run() {
        Flux.fromIterable(inputs)
                .flatMap(fullyQualifiedTopic -> {
                    var inputLiiklus = liiklusInstancesPerAddress.get(fullyQualifiedTopic.getGatewayAddress());
                    return inputLiiklus.subscribe(subscribeRequestForInput(fullyQualifiedTopic.getTopic()))
                            .filter(SubscribeReply::hasAssignment)
                            .map(SubscribeReply::getAssignment)
                            .map(Processor::receiveRequestForAssignment)
                            .flatMap(inputLiiklus::receive)
                            .map(receiveReply -> toRiffSignal(receiveReply, fullyQualifiedTopic));
                })
                .compose(this::riffWindowing)
                .map(this::invoke)
                .concatMap(flux ->
                        flux.concatMap(m -> {
                            var next = m.getData();
                            var output = outputs.get(next.getResultIndex());
                            var outputLiiklus = liiklusInstancesPerAddress.get(output.getGatewayAddress());
                            return outputLiiklus.publish(createPublishRequest(next, output.getTopic()));
                        })
                )
                .blockLast();
    }

    private static Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> indexByAddress(
            Collection<FullyQualifiedTopic> fullyQualifiedTopics) {
        return fullyQualifiedTopics.stream()
                .map(FullyQualifiedTopic::getGatewayAddress)
                .distinct()
                .map(address -> Map.entry(
                        address,
                        NettyChannelBuilder.forTarget(address)
                                .usePlaintext()
                                .build()))
                .map(channelEntry -> Map.entry(
                        channelEntry.getKey(),
                        ReactorLiiklusServiceGrpc.newReactorStub(channelEntry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Flux<OutputSignal> invoke(Flux<InputFrame> in) {
        var start = InputSignal.newBuilder()
                .setStart(StartFrame.newBuilder()
                        .addAllExpectedContentTypes(this.outputContentTypes)
                        .build())
                .build();

        return riffStub.invoke(Flux.concat(
                Flux.just(start), //
                in.map(frame -> InputSignal.newBuilder().setData(frame).build())));
    }

    /**
     * This converts an RPC representation of an {@link OutputFrame} to an at-rest {@link Message}, and creates a publish request for it.
     */
    private PublishRequest createPublishRequest(OutputFrame next, String topic) {
        Message msg = Message.newBuilder()
                .setPayload(next.getPayload())
                .setContentType(next.getContentType())
                .putAllHeaders(next.getHeadersMap())
                .build();

        return PublishRequest.newBuilder()
                .setValue(msg.toByteString())
                .setTopic(topic)
                .build();
    }

    private static ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
        return ReceiveRequest.newBuilder().setAssignment(assignment).build();
    }

    private <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
        return linear.window(Duration.ofSeconds(60));
    }

    /**
     * This converts a liiklus received message (representing an at-rest riff {@link Message}) into an RPC {@link InputFrame}.
     */
    private InputFrame toRiffSignal(ReceiveReply receiveReply, FullyQualifiedTopic fullyQualifiedTopic) {
        var inputIndex = inputs.indexOf(fullyQualifiedTopic);
        if (inputIndex == -1) {
            throw new RuntimeException("Unknown topic: " + fullyQualifiedTopic);
        }
        ByteString bytes = receiveReply.getRecord().getValue();
        try {
            Message message = Message.parseFrom(bytes);
            return InputFrame.newBuilder()
                    .setPayload(message.getPayload())
                    .setContentType(message.getContentType())
                    .setArgIndex(inputIndex)
                    .build();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

    }

    private SubscribeRequest subscribeRequestForInput(String topic) {
        return SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setGroup(group)
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
                .build();
    }

    private static List<String> parseContentTypes(String json, int outputCount) {
        try {
            List<String> contentTypes = new ObjectMapper().readValue(json, StreamOutputContentTypes.class).getContentTypes();
            int actualSize = contentTypes.size();
            if (actualSize != outputCount) {
                throw new RuntimeException(
                        String.format("Expected %d output stream content type(s), got %d.%n\tSee %s", outputCount, actualSize, json)
                );
            }
            return contentTypes;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}