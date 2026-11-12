package bikestore;

import java.time.Instant;

/** Log a consola con pedidoId, timestamp y thread. */
public final class Log {
    private Log() {}

    public static void info(String pedidoId, String mensaje) {
        System.out.printf("[%s] pedidoId=%s thread=%s | %s%n",
                Instant.now(), pedidoId, Thread.currentThread().getName(), mensaje);
    }
}
