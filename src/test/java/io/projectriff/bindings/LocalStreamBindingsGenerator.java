package io.projectriff.bindings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.projectriff.processor.Processor;
import io.projectriff.processor.StreamBinding;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static picocli.CommandLine.ArgGroup;
import static picocli.CommandLine.Option;


interface StreamBindingOption {

	GatewayAddress gatewayAddress();

	String topic();

	String namespace();

	default String qualifiedTopicName() {
		return String.format("%s_%s", namespace(), topic());
	}

	default GatewayAddress gatewayAddressOrDefault(GatewayAddress address) {
		GatewayAddress result = gatewayAddress();
		if (result != null) {
			return result;
		}
		return requireNonNull(address);
	}
}

class InputStreamBindingOption implements StreamBindingOption {

	/**
	 * Address of the gateway coordinating access to the underlying stream
	 */
	@Option(names = {"-ig", "--input-gateway"}, paramLabel = "INPUT GATEWAY",
			description = "streaming gateway address for this input binding")
	GatewayAddress gatewayAddress;

	/**
	 * Topic name for this stream
	 */
	@Option(required = true,
			names = {"-it", "--input-topic"}, paramLabel = "INPUT TOPIC",
			description = "topic name for this input binding")
	String topicName;

	/**
	 * Namespace for this stream
	 */
	@Option(names = {"-in", "--input-namespace"}, paramLabel = "INPUT STREAM NAMESPACE",
			description = "namespace for this input binding stream")
	String namespace = "default";

	@Override
	public GatewayAddress gatewayAddress() {
		return gatewayAddress;
	}

	@Override
	public String topic() {
		return topicName;
	}

	@Override
	public String namespace() {
		return namespace;
	}

	public StreamBinding toStreamBinding(GatewayAddress defaultGateway) {
		return new StreamBinding(gatewayAddressOrDefault(defaultGateway).string(), qualifiedTopicName());
	}
}

class OutputStreamBindingOption implements StreamBindingOption {

	/**
	 * Address of the gateway coordinating access to the underlying stream
	 */
	@Option(names = {"-og", "--output-gateway"}, paramLabel = "OUTPUT GATEWAY",
			description = "streaming gateway address for this output binding")
	GatewayAddress gatewayAddress;

	/**
	 * Topic name for this stream
	 */
	@Option(required = true,
			names = {"-ot", "--output-topic"}, paramLabel = "OUTPUT TOPIC",
			description = "topic name for this output binding")
	String topicName;

	/**
	 * Namespace for this stream
	 */
	@Option(names = {"-on", "--output-namespace"}, paramLabel = "OUTPUT STREAM NAMESPACE",
			description = "namespace for this output binding stream")
	String namespace = "default";

	/**
	 * Acceptable media type representation of the data at rest in this stream
	 */
	@Option(names = {"-a", "--accept"}, paramLabel = "ACCEPT",
			description = "acceptable media type for this output topic")
	String acceptableMediaType = "*/*";

	@Override
	public GatewayAddress gatewayAddress() {
		return gatewayAddress;
	}

	@Override
	public String topic() {
		return topicName;
	}

	@Override
	public String namespace() {
		return namespace;
	}

	public StreamBinding toStreamBinding(GatewayAddress defaultGateway) {
		Map<String, String> metadata = new HashMap<>(1, 1.0f);
		metadata.put(StreamBinding.CONTENT_TYPE, acceptableMediaType);
		return new StreamBinding(gatewayAddressOrDefault(defaultGateway).string(), qualifiedTopicName(), metadata);
	}
}

class GatewayAddress {

	private final String address;

	private final int port;

	private GatewayAddress(String address, int port) {
		this.address = address;
		this.port = port;
	}

	static GatewayAddress parse(String address) {
		String[] parts = address.split(":");
		if (parts.length == 1) {
			return new GatewayAddress(address, 6565);
		}
		int port = Integer.parseInt(parts[parts.length - 1]);
		String host = address.substring(0, address.length() - (":" + port).length());
		return new GatewayAddress(host, port);
	}

	public String string() {
		return String.format("%s:%d", address, port);
	}
}

/**
 * CLI that generates the necessary bindings for riff streams on local disk.<br/>
 *
 * Example invocations:
 * <code><pre>
 * # with explicit pre-existing parent directory, 1 shared gateway, 1 input stream, n output streams
 *
 * ./streaming-bindings-generator --base-directory $HOME/Desktop/bindings \
 * 	--default-gateway franz-gateway-4v8fj:6565 \
 * 	--input-topic in \
 * 	--output-topic out1 --accept application/json \
 * 	--output-topic out2 --accept text/csv
 *
 *
 * # with generated parent directory, 1 gateway / stream, n input streams, 1 output stream
 *
 * ./streaming-bindings-generator \
 * 	--input-topic in1 --input-gateway franz-beckenbauer-4v8fj:6565 \
 * 	--input-topic in2 --input-gateway franz-ferdinand-4v8fj:6565 \
 * 	--output-topic out --output-gateway franz-kappa-4v8fj:6565 --accept application/json
 *
 *
 * # avoid mixing default gateway and explicit gateway / stream as this can lead to surprising behavior
 * # in the following example, the explicit output gateway will actually be assigned to the first output,
 * # not the second one
 *
 * ./streaming-bindings-generator --base-directory $HOME/Desktop/bindings \
 * 	--default-gateway franz-gateway-4v8fj:6565 \
 * 	--input-topic in \
 * 	--output-topic out1 --accept application/json \
 * 	--output-topic out2 --accept text/csv --output-gateway franz-ferdinand-4v8fj:6565
 * </pre></code>
 */
@Command(name = "stream-bindings-generator", mixinStandardHelpOptions = true, sortOptions = false, usageHelpWidth = 120)
public class LocalStreamBindingsGenerator implements Runnable {

	/**
	 * Root directory of the stream binding file tree (must be present on the disk)
	 * The absolute path of this directory is the value to configure for the {@link Processor} to consume the generated bindings
	 * Note: this directory is emptied at the beginning of each run
	 */
	@Option(names = {"-b", "--base-directory"}, paramLabel = "BASE DIR",
			description = "parent directory (defaults to temporary directory)")
	File baseDirectory = defaultTempDir();

	/**
	 * Default gateway address to use when specific input/output stream binding do not specify one
	 */
	@Option(names = {"-g", "--default-gateway"}, paramLabel = "DEFAULT GATEWAY",
			description = "default gateway (optional if all bindings provide a gateway")
	GatewayAddress defaultGatewayAddress;

	/**
	 * Input bindings (at least 1 is required)
	 * @see InputStreamBindingOption
	 */
	@ArgGroup(exclusive = false, heading = "INPUT BINDINGS (1..n)", multiplicity = "1..*")
	InputStreamBindingOption[] inputBindings;

	/**
	 * Output bindings (at least 1 is required)
	 * @see InputStreamBindingOption
	 */
	@ArgGroup(exclusive = false, heading = "OUTPUT BINDINGS (1..n)", multiplicity = "1..*")
	OutputStreamBindingOption[] outputBindings;

	/**
	 * Generates the bindings file tree and writes the corresponding data
	 * This is idempotent
	 */
	public void run() {
		if (!baseDirectory.exists() || !baseDirectory.isDirectory() || !baseDirectory.canWrite()) {
			throw new IllegalArgumentException(String
					.format("%s should be a writable directory", baseDirectory.getAbsolutePath()));
		}
		BindingValidator.validateBindings(defaultGatewayAddress, inputBindings, outputBindings);

		StreamBindingWriter streamBindingWriter = StreamBindingWriter.init(baseDirectory);
		streamBindingWriter.writeInputStreamBindings(Arrays.stream(inputBindings)
				.map(binding -> binding.toStreamBinding(defaultGatewayAddress))
				.collect(Collectors.toList()));
		streamBindingWriter.writeOutputStreamBindings(Arrays.stream(outputBindings)
				.map(binding -> binding.toStreamBinding(defaultGatewayAddress))
				.collect(Collectors.toList()));

		System.out.println(String.format("Please configure %s envvar to point to %s when running %s",
				Processor.CNB_BINDINGS,
				baseDirectory.getAbsolutePath(),
				Processor.class.getSimpleName()));

	}

	public static void main(String... args) {
		CommandLine commandLine = new CommandLine(new LocalStreamBindingsGenerator());
		commandLine.registerConverter(GatewayAddress.class, GatewayAddress::parse);
		System.exit(commandLine.execute(args));
	}

	private static File defaultTempDir() {
		try {
			return Files.createTempDirectory("local-bindings").toFile();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}

class BindingValidator {

	/*
	 * Make sure every binding explicitly specify a gateway or inherit the one specified as default
	 */
	static void validateBindings(GatewayAddress defaultGateway, InputStreamBindingOption[] inputs, OutputStreamBindingOption[] outputs) {
		if (defaultGateway != null) {
			return;
		}

		Predicate<StreamBindingOption> invalidPredicate = binding -> binding.gatewayAddress() == null;
		List<Integer> invalidInputBindings = indicesOfInvalidBindings(inputs, invalidPredicate);
		List<Integer> invalidOutputBindings = indicesOfInvalidBindings(outputs, invalidPredicate);
		if (invalidInputBindings.size() + invalidOutputBindings.size() == 0) {
			return;
		}

		String prefix = "\tMissing gateway address for %s bindings at indices:";
		String inputMessage = join(String.format(prefix, "input"), invalidInputBindings);
		String outputMessage = join(String.format(prefix, "output"), invalidOutputBindings);
		throw new IllegalArgumentException(
				String.format("Invalid binding specification.\n%s%s", inputMessage, outputMessage)
		);
	}

	private static List<Integer> indicesOfInvalidBindings(StreamBindingOption[] bindings, Predicate<StreamBindingOption> validator) {
		return IntStream.range(0, bindings.length)
				.filter(i -> validator.test(bindings[i]))
				.boxed()
				.collect(toList());
	}

	private static String join(String prefix, List<Integer> indices) {
		if (indices.isEmpty()) {
			return "";
		}
		return indices.stream().map(Object::toString).collect(joining(",", prefix, "\n"));
	}
}
