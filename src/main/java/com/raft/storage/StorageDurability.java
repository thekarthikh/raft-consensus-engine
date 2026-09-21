package com.raft.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/** Best-effort directory forcing only where the filesystem provider supports it. */
final class StorageDurability {
    private static final Logger logger = LoggerFactory.getLogger(StorageDurability.class);
    private static final AtomicBoolean warned = new AtomicBoolean();

    private StorageDurability() {}

    static boolean forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
            return true;
        } catch (AccessDeniedException e) {
            // Windows' default NIO provider cannot open directories as FileChannels.
            // Do not swallow permission failures on systems that normally support this.
            if (!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")) throw e;
            return unsupported(directory, e);
        } catch (UnsupportedOperationException e) {
            return unsupported(directory, e);
        }
    }

    private static boolean unsupported(Path directory, Exception cause) {
        if (warned.compareAndSet(false, true))
            logger.warn("Directory force unavailable for {}: {}. File forcing/atomic rename alone "
                    + "do not establish power-loss durability.", directory, cause.toString());
        return false;
    }
}
