package io.projectriff.processor;

import com.github.bsideup.liiklus.protocol.*;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.grpc.netty.NettyChannelBuilder;
import io.projectriff.invoker.rpc.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.function.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.bsideup.liiklus.protocol.ReceiveReply.ReplyCase.LIIKLUS_EVENT_RECORD;
import static com.github.bsideup.liiklus.protocol.ReceiveRequest.ContentFormat.LIIKLUS_EVENT;
import static com.github.bsideup.liiklus.protocol.SubscribeRequest.AutoOffsetReset.EARLIEST;
import static com.github.bsideup.liiklus.protocol.SubscribeRequest.AutoOffsetReset.LATEST;

/**
 * Main driver class for the streaming processor.
 *
 * <p>Continually pumps data from one or several input streams (serialized as CloudEvents),
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
     * ENV VAR key holding the directory path where streams metadata can be found.
     */
    public static final String CNB_BINDINGS = "CNB_BINDINGS";

    /**
     * ENV VAR key holding the address of the function RPC, as a {@code host:port} string.
     */
    private static final String FUNCTION = "FUNCTION";

    /**
     * ENV VAR key holding the logical names for input parameter names, as a comma separated list of strings.
     */
    private static final String INPUT_NAMES = "INPUT_NAMES";

    /**
     * ENV VAR key holding the logical names for output result names, as a comma separated list of strings.
     */
    private static final String OUTPUT_NAMES = "OUTPUT_NAMES";

    /**
     * ENV VAR key holding the start offsets for each input stream, as a comma separated list of either "earliest" or "latest".
     */
    private static final String INPUT_START_OFFSETS = "INPUT_START_OFFSETS";

    /**
     * ENV VAR key holding the consumer group string this process should use.
     */
    private static final String GROUP = "GROUP";

    /**
     * The number of retries when testing http connection to the function.
     */
    private static final int NUM_RETRIES = 20;

    /**
     * Keeps track of a single gRPC stub per gateway address.
     */
    private final Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> liiklusInstancesPerAddress;

    /**
     * The ordered input streams for the function, in parsed form.
     */
    private final List<StreamBinding> inputs;

    /**
     * The ordered output streams for the function, in parsed form.
     */
    private final List<StreamBinding> outputs;

    /**
     * The ordered logical names for input parameters of the function.
     */
    private final List<String> inputNames;

    /**
     * For each input stream, whether to subscribe at earliest or latest offset.
     */
    private final List<String> startOffsets;

    /**
     * The ordered logical names for output results of the function.
     */
    private final List<String> outputNames;

    /**
     * The ordered list of expected content-types for function results.
     */
    private final List<String> outputContentTypes;

    /**
     * The consumer group string this process will use to identify itself when reading from the input streams.
     */
    private final String group;

    /**
     * This is used in a shutdown hook to force completion of the input signals Flux via takeUntilOther().
     */
    private UnicastProcessor killSignal = UnicastProcessor.create();

    /**
     * The RPC stub used to communicate with the function process.
     *
     * @see "riff-rpc.proto for the wire format and service definition"
     */
    private final ReactorRiffGrpc.ReactorRiffStub riffStub;

    public static void main(String[] args) throws Exception {

        long t0 = System.currentTimeMillis();

        checkEnvironmentVariables();

        Hooks.onOperatorDebug();

        String functionAddress = System.getenv(FUNCTION);

        List<String> startOffsets = Arrays.asList(System.getenv(INPUT_START_OFFSETS).split(","));
        List<String> inputNames = Arrays.asList(System.getenv(INPUT_NAMES).split(","));
        if (startOffsets.size() != inputNames.size()) {
            System.err.format("%s (%d element(s)) should have as many elements as %s (%d element(s))", INPUT_START_OFFSETS, startOffsets.size(), INPUT_NAMES, inputNames.size());
            System.exit(2);
        }
        List<String> outputNames = Arrays.asList(System.getenv(OUTPUT_NAMES).split(","));


        StreamBindingReader streamBindingReader = StreamBindingReader.init(new File(System.getenv(CNB_BINDINGS)));
        List<StreamBinding> inputStreamBindings = streamBindingReader.readInputStreamBindings(inputNames.size());
        List<StreamBinding> outputStreamBindings = streamBindingReader.readOutputStreamBindings(outputNames.size());

        assertHttpConnectivity(functionAddress);
        Channel fnChannel = NettyChannelBuilder.forTarget(functionAddress)
                .usePlaintext()
                .build();

        Processor processor = new Processor(
                inputStreamBindings,
                outputStreamBindings,
                inputNames,
                startOffsets,
                outputNames,
                outputStreamBindings.stream().map(binding -> binding.getMetadata().get(StreamBinding.CONTENT_TYPE)).collect(Collectors.toList()),
                System.getenv(GROUP),
                ReactorRiffGrpc.newReactorStub(fnChannel));

        System.out.format("Connected to %s, after %d ms\n", functionAddress, System.currentTimeMillis() - t0);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                processor.killSignal.sink().complete();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }
        });


        processor.run();

    }

    private static void checkEnvironmentVariables() {
        List<String> envVars = Arrays.asList(FUNCTION, GROUP, INPUT_NAMES, OUTPUT_NAMES, INPUT_START_OFFSETS, CNB_BINDINGS);
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

    private Processor(List<StreamBinding> inputs,
                      List<StreamBinding> outputs,
                      List<String> inputNames,
                      List<String> startOffsets,
                      List<String> outputNames,
                      List<String> outputContentTypes,
                      String group,
                      ReactorRiffGrpc.ReactorRiffStub riffStub) {

        this.inputs = inputs;
        this.outputs = outputs;
        this.inputNames = inputNames;
        this.startOffsets = startOffsets;
        this.outputNames = outputNames;
        Set<StreamBinding> allGateways = new HashSet<>(inputs);
        allGateways.addAll(outputs);

        this.liiklusInstancesPerAddress = indexByAddress(allGateways);
        this.outputContentTypes = outputContentTypes;
        this.riffStub = riffStub;
        this.group = group;
    }

    public void run() {
        Flux.fromIterable(inputs)
                .zipWithIterable(startOffsets)
                .flatMap(inputTopic -> {
                    ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub inputLiiklus = liiklusInstancesPerAddress.get(inputTopic.getT1().getGatewayAddress());
                    return inputLiiklus.subscribe(subscribeRequestForInput(inputTopic))
                            .filter(SubscribeReply::hasAssignment)
                            .map(SubscribeReply::getAssignment)
                            .flatMap(
                                    assignment -> inputLiiklus
                                            .receive(receiveRequestForAssignment(assignment))
                                            .delayUntil(receiveReply -> ack(inputTopic.getT1(), inputLiiklus, receiveReply, assignment))
                            )
                            .map(receiveReply -> toRiffSignal(receiveReply, inputTopic.getT1()));
                })
                .takeUntilOther(killSignal)
                .transform(this::riffWindowing)
                .map(this::invoke)
                .concatMap(this::extractWindowResults)
                .concatMap(this::publishOutput)
                .blockLast();
    }

    private <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
        return linear.window(Duration.ofSeconds(60));
    }

    private Flux<OutputSignal> extractWindowResults(Flux<OutputSignal> outputSignals) {
        return outputSignals;
    }

    private Publisher<? extends PublishReply> publishOutput(OutputSignal m) {
        OutputFrame next = m.getData();
        StreamBinding output = outputs.get(next.getResultIndex());
        ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub outputLiiklus = liiklusInstancesPerAddress.get(output.getGatewayAddress());
        return outputLiiklus.publish(createPublishRequest(next, output.getTopic()));
    }

    private Mono<Empty> ack(StreamBinding topic, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub stub, ReceiveReply receiveReply, Assignment assignment) {
        System.out.format("ACKing %s for group %s: offset=%d, part=%d%n", topic.getTopic(), this.group, receiveReply.getRecord().getOffset(), assignment.getPartition());
        return stub.ack(AckRequest.newBuilder()
                .setGroup(this.group)
                .setOffset(receiveReply.getRecord().getOffset())
                .setPartition(assignment.getPartition())
                .setTopic(topic.getTopic())
                .build());
    }

    private static Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> indexByAddress(
            Collection<StreamBinding> streamBindings) {
        return streamBindings.stream()
                .map(StreamBinding::getGatewayAddress)
                .distinct()
                .collect(Collectors.toMap(
                        address -> address,
                        address -> ReactorLiiklusServiceGrpc.newReactorStub(
                                NettyChannelBuilder.forTarget(address)
                                        .usePlaintext()
                                        .build())
                        )
                )
                ;
    }

    private Flux<OutputSignal> invoke(Flux<InputFrame> in) {
        InputSignal start = InputSignal.newBuilder()
                .setStart(StartFrame.newBuilder()
                        .addAllExpectedContentTypes(this.outputContentTypes)
                        .addAllInputNames(this.inputNames)
                        .addAllOutputNames(this.outputNames)
                        .build())
                .build();

        return riffStub.invoke(Flux.concat(
                Flux.just(start), //
                in.map(frame -> InputSignal.newBuilder().setData(frame).build())));
    }

    /**
     * This converts an RPC representation of an {@link OutputFrame} to an at-rest CloudEvent, and creates a publish request for it.
     */
    private PublishRequest createPublishRequest(OutputFrame next, String topic) {
        return PublishRequest.newBuilder()
                .setLiiklusEvent(
                        LiiklusEvent.newBuilder()
                                .setData(next.getPayload())
                                .setDataContentType(next.getContentType())
                                .setType("riff-event") // TODO
                                .setSource(this.group) // TODO
                                .putAllExtensions(next.getHeadersMap())
                                .setId(UUID.randomUUID().toString())
                )
                .setTopic(topic)
                .build();
    }

    private static ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
        return ReceiveRequest.newBuilder().setAssignment(assignment).setFormat(LIIKLUS_EVENT).build();
    }

    /**
     * This converts a liiklus received message (in CloudEvent format) into an RPC {@link InputFrame}.
     */
    private InputFrame toRiffSignal(ReceiveReply receiveReply, StreamBinding streamBinding) {
        int inputIndex = inputs.indexOf(streamBinding);
        if (inputIndex == -1) {
            throw new RuntimeException("Unknown topic: " + streamBinding);
        }
        if (receiveReply.getReplyCase() == LIIKLUS_EVENT_RECORD) {
            LiiklusEvent event = receiveReply.getLiiklusEventRecord().getEvent();
            return InputFrame.newBuilder()
                    .setPayload(event.getData())
                    .setContentType(event.getDataContentType())
                    .setArgIndex(inputIndex)
                    .putAllHeaders(event.getExtensionsMap())
                    .build();
        } else {
            throw new RuntimeException("Expected messages in CloudEvent format, got " + receiveReply.getReplyCase());
        }

    }

    private SubscribeRequest subscribeRequestForInput(Tuple2<StreamBinding, String> topicAddressAndOffset) {
        return SubscribeRequest.newBuilder()
                .setTopic(topicAddressAndOffset.getT1().getTopic())
                .setGroup(group)
                .setAutoOffsetReset(topicAddressAndOffset.getT2().equals("earliest") ? EARLIEST : LATEST)
                .build();
    }
}