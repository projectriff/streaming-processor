package io.projectriff.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.projectriff.processor.StreamOutputContentTypes.StreamOutputContentType;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamOutputContentTypesTest {

    @Test
    public void deserializes_single_stream_output() throws IOException {
        StreamOutputContentType streamOutputContentType = new ObjectMapper().readValue("{\n" +
                "  \"outputIndex\": 42,\n" +
                "  \"contentType\": \"text/csv\"\n" +
                "}", StreamOutputContentType.class);

        assertThat(streamOutputContentType.getOutputIndex()).isEqualTo(42);
        assertThat(streamOutputContentType.getContentType()).isEqualTo("text/csv");
    }

    @Test
    public void deserializes_stream_outputs() throws IOException {
        StreamOutputContentTypes streamOutputContentType = new ObjectMapper().readValue("[\n" +
                "  {\n" +
                "    \"outputIndex\": 2,\n" +
                "    \"contentType\": \"text/csv\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"outputIndex\": 0,\n" +
                "    \"contentType\": \"application/json\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"outputIndex\": 1,\n" +
                "    \"contentType\": \"text/plain\"\n" +
                "  }\n" +
                "]", StreamOutputContentTypes.class);

        assertThat(streamOutputContentType.getContentTypes()).containsExactly("application/json", "text/plain", "text/csv");
    }

}