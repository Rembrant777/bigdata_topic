package deprecated.tobe.deleted.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class TimeClient {
    private static final Logger LOG = LoggerFactory.getLogger(TimeClient.class);

    public static void main(String[] args) {
        int port = 8080;
        if (Objects.nonNull(args) && args.length > 0) {
            try {
                port = Integer.valueOf(args[0]);
            } catch (NumberFormatException e) {
                LOG.error("#main got error!", e);
            }
        }

        new Thread(
                new TimeClientHandle("localhost", port),
                "TimeClient-001")
                .start();
    }
}
