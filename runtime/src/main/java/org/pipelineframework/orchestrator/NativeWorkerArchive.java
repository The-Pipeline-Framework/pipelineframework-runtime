package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

/** Materializes a verified native application archive without interpreting archive permissions or links. */
final class NativeWorkerArchive {
    private static final int MAX_FILES = 512;
    private static final int MAX_BYTES = 32 * 1024 * 1024;
    private static final Pattern PATH_COMPONENT = Pattern.compile("[A-Za-z0-9_.@-]+");

    private final Path root;
    private final Map<Path, FilePin> files;
    private final Set<Path> directories;

    private NativeWorkerArchive(Path root, Map<Path, FilePin> files) {
        this.root = root;
        this.files = Map.copyOf(files);
        Set<Path> parents = new HashSet<>();
        parents.add(root);
        for (Path path : files.keySet()) {
            Path parent = path.getParent();
            while (parents.add(parent)) {
                parent = parent.getParent();
            }
        }
        this.directories = Set.copyOf(parents);
    }

    static NativeWorkerArchive materialize(Path source, Path destination, NativeWorkerReleaseBinding.Language language)
            throws IOException {
        Path root = Files.createDirectory(destination);
        Map<Path, FilePin> files = new HashMap<>();
        int remaining = MAX_BYTES;
        try (var zip = new ZipFile(source.toFile())) {
            if (zip.size() > MAX_FILES) {
                throw new IOException("Native worker archive exceeds the file limit");
            }
            var entries = zip.entries();
            while (entries.hasMoreElements()) {
                var entry = entries.nextElement();
                String name = entry.getName();
                if (entry.isDirectory() || !safeName(name) || !language.includesArchiveFile(name)) {
                    throw new IOException("Unexpected path in native worker archive: " + name);
                }
                Path file = root.resolve(name);
                if (files.containsKey(file)) {
                    throw new IOException("Duplicate file in native worker archive: " + name);
                }
                byte[] bytes;
                try (var input = zip.getInputStream(entry)) {
                    bytes = input.readNBytes(remaining + 1);
                }
                if (bytes.length > remaining) {
                    throw new IOException("Native worker archive exceeds the expanded byte limit");
                }
                remaining -= bytes.length;
                Files.createDirectories(file.getParent());
                Files.write(file, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                files.put(file, new FilePin(bytes.length, sha256(bytes)));
            }
        }
        for (String required : language.requiredArchiveFiles()) {
            if (!files.containsKey(root.resolve(required))) {
                throw new IOException("Native worker archive is missing " + required);
            }
        }
        return new NativeWorkerArchive(root, files);
    }

    Path verifyTree() throws IOException {
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) || !root.equals(root.toRealPath())) {
            throw new IOException("Native worker runnable root is unavailable");
        }
        var seenFiles = new HashSet<Path>();
        var seenDirectories = new HashSet<Path>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attributes) throws IOException {
                if (!directories.contains(path)) {
                    throw changed(path);
                }
                seenDirectories.add(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attributes) throws IOException {
                var pin = files.get(path);
                if (pin == null || !attributes.isRegularFile() || attributes.size() != pin.size()) {
                    throw changed(path);
                }
                try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
                    byte[] bytes = input.readNBytes(pin.size() + 1);
                    if (bytes.length != pin.size() || !sha256(bytes).equals(pin.digest())) {
                        throw changed(path);
                    }
                }
                seenFiles.add(path);
                return FileVisitResult.CONTINUE;
            }
        });
        if (!seenFiles.equals(files.keySet()) || !seenDirectories.equals(directories)) {
            throw new IOException("Prepared native worker tree is missing a pinned entry");
        }
        return root;
    }

    static void deleteTree(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException failure) throws IOException {
                if (failure != null) {
                    throw failure;
                }
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    static String sha256(byte[] bytes) {
        try {
            return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private IOException changed(Path path) {
        return new IOException("Prepared native worker tree changed at " + root.relativize(path));
    }

    private static boolean safeName(String name) {
        return Arrays.stream(name.split("/", -1)).allMatch(part -> PATH_COMPONENT.matcher(part).matches()
            && !Set.of(".", "..", "__pycache__").contains(part))
            && !name.endsWith(".pyc") && !name.endsWith(".pyo");
    }

    private record FilePin(int size, String digest) {
    }
}
