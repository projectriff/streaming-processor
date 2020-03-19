package io.projectriff.bindings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import io.projectriff.processor.StreamBinding;
import io.projectriff.processor.StreamBindingPaths;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public class StreamBindingWriter {

	private final File rootDirectory;

	private StreamBindingWriter(File rootDirectory) {
		this.rootDirectory = rootDirectory;
	}

	public static StreamBindingWriter init(File rootDirectory) {
		purge(rootDirectory);
		return new StreamBindingWriter(rootDirectory);
	}

	public void writeInputStreamBindings(List<StreamBinding> inputStreamBindings) {
		IntStream.range(0, inputStreamBindings.size())
				.forEach(i -> {
					StreamBinding streamBinding = inputStreamBindings.get(i);
					Path bindingRootDirectory = rootDirectory.toPath().resolve(StreamBindingPaths.inputBindingPath(i));

					tryWriteBinding(bindingRootDirectory, streamBinding);
				});
	}

	public void writeOutputStreamBindings(List<StreamBinding> outputStreamBindings) {
		IntStream.range(0, outputStreamBindings.size())
				.forEach(i -> {
					StreamBinding streamBinding = outputStreamBindings.get(i);
					Path bindingRootDirectory = rootDirectory.toPath().resolve(StreamBindingPaths.outputBindingPath(i));

					tryWriteBinding(bindingRootDirectory, streamBinding);
					tryWriteMetadata(bindingRootDirectory, streamBinding.getMetadata());
				});

	}

	private static void purge(File dir) {
		for (File file : requireNonNull(dir.listFiles())) {
			if (file.isDirectory()) {
				purge(file);
			}
			if (!file.delete()) {
				throw new RuntimeException(String.format("Could not delete %s", file.getAbsolutePath()));
			}
		}
	}

	private static void tryWriteBinding(Path root, StreamBinding binding) {
		try {
			writeStreamBinding(root, binding);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void tryWriteMetadata(Path root, Map<String, String> metadata) {
		try {
			writeMetadata(root, metadata);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void writeStreamBinding(Path root, StreamBinding binding) throws IOException {
		File secretDir = new File(root.toFile(), "secret");
		Files.createDirectories(secretDir.toPath());

		File gateway = new File(secretDir, "gateway");
		byte[] gatewayAddress = binding.getGatewayAddress().getBytes(US_ASCII);
		Files.write(gateway.toPath(), gatewayAddress);

		File topic = new File(secretDir, "topic");
		Files.write(topic.toPath(), binding.getTopic().getBytes(US_ASCII));
	}

	private static void writeMetadata(Path root, Map<String, String> metadata) throws IOException {
		File metadataDir = new File(root.toFile(), "metadata");
		Files.createDirectory(metadataDir.toPath());

		File contentType = new File(metadataDir, "contentType");
		byte[] gatewayAddress = requireNonNull(metadata.get(StreamBinding.CONTENT_TYPE)).getBytes(US_ASCII);
		Files.write(contentType.toPath(), gatewayAddress);
	}
}
