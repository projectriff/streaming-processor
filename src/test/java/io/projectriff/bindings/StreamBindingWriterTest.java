package io.projectriff.bindings;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.projectriff.processor.StreamBinding;
import io.projectriff.processor.StreamBindingReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamBindingWriterTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private List<StreamBinding> initialInputs;

	private List<StreamBinding> initialOutputs;

	private File baseDirectory;

	@Before
	public void prepare() throws IOException {
		initialInputs = Arrays.asList(
			new StreamBinding("gateway1.example.com:6565", "in1"),
			new StreamBinding("gateway2.example.com:6565", "in2")
		);
		initialOutputs = Arrays.asList(
			new StreamBinding("gateway3.example.com:6565", "out1", accept("application/json")),
			new StreamBinding("gateway3.example.com:6565", "out2", accept("text/*")),
			new StreamBinding("gateway3.example.com:6565", "out3", accept("image/*"))
		);
		baseDirectory = temporaryFolder.newFolder("local-bindings");
	}

	@Test
	public void writes_and_read_input_bindings() {
		StreamBindingWriter writer = StreamBindingWriter.init(baseDirectory);
		writer.writeInputStreamBindings(initialInputs);
		StreamBindingReader reader = StreamBindingReader.init(baseDirectory);

		List<StreamBinding> result = reader.readInputStreamBindings(initialInputs.size());

		assertThat(result).isEqualTo(initialInputs);
	}

	@Test
	public void writes_and_read_output_bindings() {
		StreamBindingWriter writer = StreamBindingWriter.init(baseDirectory);
		writer.writeOutputStreamBindings(initialOutputs);
		StreamBindingReader reader = StreamBindingReader.init(baseDirectory);

		List<StreamBinding> result = reader.readOutputStreamBindings(initialOutputs.size());

		assertThat(result).isEqualTo(initialOutputs);
	}

	private Map<String, String> accept(String mediaType) {
		Map<String, String> result = new HashMap<>(1, 1.f);
		result.put(StreamBinding.CONTENT_TYPE, mediaType);
		return result;
	}
}
