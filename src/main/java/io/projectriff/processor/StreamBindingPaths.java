package io.projectriff.processor;

import java.nio.file.Path;
import java.nio.file.Paths;

public class StreamBindingPaths {

	public static Path inputBindingPath(int bindingIndex) {
		return numberedPath("input", bindingIndex);
	}

	public static Path outputBindingPath(int bindingIndex) {
		return numberedPath("output", bindingIndex);
	}

	private static Path numberedPath(String prefix, int bindingIndex) {
		return Paths.get(String.format(prefix + "_%03d", bindingIndex));
	}
}
