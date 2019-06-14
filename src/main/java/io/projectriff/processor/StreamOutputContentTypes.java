package io.projectriff.processor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingInt;

/**
 * Used to serialize configuration about output content-types.
 *
 * @author Florent Biville
 */
public class StreamOutputContentTypes {

    private final List<StreamOutputContentType> outputs;

    @JsonCreator
    public StreamOutputContentTypes(@JsonDeserialize(using = ArrayToListDeserializer.class) List<StreamOutputContentType> outputs) {
        outputs.sort(comparingInt(StreamOutputContentType::getOutputIndex));
        this.outputs = outputs;
    }

    public List<String> getContentTypes() {
        return this.outputs.stream().map(StreamOutputContentType::getContentType).collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamOutputContentTypes that = (StreamOutputContentTypes) o;
        return Objects.equals(outputs, that.outputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputs);
    }

    static class ArrayToListDeserializer extends JsonDeserializer {
        @Override
        public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            return Arrays.asList(jsonParser.readValueAs(StreamOutputContentType[].class));
        }
    }

    static class StreamOutputContentType {

        private final int outputIndex;

        private final String contentType;

        @JsonCreator
        public StreamOutputContentType(@JsonProperty("outputIndex") int outputIndex,
                                       @JsonProperty("contentType") String contentType) {

            this.outputIndex = outputIndex;
            this.contentType = contentType;
        }

        public int getOutputIndex() {
            return outputIndex;
        }

        public String getContentType() {
            return contentType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamOutputContentType that = (StreamOutputContentType) o;
            return outputIndex == that.outputIndex &&
                    Objects.equals(contentType, that.contentType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(outputIndex, contentType);
        }
    }

}
