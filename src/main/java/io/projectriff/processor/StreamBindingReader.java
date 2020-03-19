package io.projectriff.processor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.projectriff.processor.StreamBindingPaths.inputBindingPath;
import static io.projectriff.processor.StreamBindingPaths.outputBindingPath;
import static java.nio.charset.StandardCharsets.UTF_8;

public class StreamBindingReader {

	private final File rootDirectory;

	private StreamBindingReader(File rootDirectory) {
		this.rootDirectory = rootDirectory;
	}

	public static StreamBindingReader init(File rootDirectory) {
		return new StreamBindingReader(rootDirectory);
	}

	public List<StreamBinding> readInputStreamBindings(int count) {
		return IntStream.range(0, count)
				.mapToObj(i -> readStreamBinding(rootDirectory.toPath().resolve(inputBindingPath(i))))
				.collect(Collectors.toList());
	}

	public List<StreamBinding> readOutputStreamBindings(int count) {
		return IntStream.range(0, count)
				.mapToObj(i -> {
					Path bindingRoot = rootDirectory.toPath().resolve(outputBindingPath(i));
					StreamBinding streamBinding = readStreamBinding(bindingRoot);
					return new StreamBinding(
							streamBinding.getGatewayAddress(),
							streamBinding.getTopic(),
							readMetadata(bindingRoot));
				})
				.collect(Collectors.toList());
	}

	private static StreamBinding readStreamBinding(Path root) {
		try {
			Path secretRoot = root.resolve("secret");
			String gateway = new String(Files.readAllBytes(secretRoot.resolve("gateway")), UTF_8);
			String topic = new String(Files.readAllBytes(secretRoot.resolve("topic")), UTF_8);
			return new StreamBinding(gateway, topic);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Map<String, String> readMetadata(Path root) {
		try {
			Map<String, String> result = new HashMap<>(1, 1.f);
			Path path = root.resolve("metadata").resolve("contentType");
			result.put(StreamBinding.CONTENT_TYPE, new String(Files.readAllBytes(path), UTF_8));
			return result;
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
