package bikestore;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * API "Ordenar": POST http://localhost:8080/ordenar
 * Body: {"cliente":"Mateo","email":"mateo@correo.com","total":349.9}
 * Convierte la petición en un mensaje JSON y lo publica en la cola pedidos.pago.
 */
public class OrderProducer {

    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws Exception {
        Connection conn = Config.conectar();
        try (Channel ch = conn.createChannel()) {
            Config.declarar(ch);
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.createContext("/ordenar", ex -> {
            try {
                if (!"POST".equals(ex.getRequestMethod())) {
                    responder(ex, 405, "{\"error\":\"usa POST\"}");
                    return;
                }
                String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Pedido p = GSON.fromJson(body, Pedido.class);
                if (p == null || p.email == null || p.email.isBlank()) {
                    responder(ex, 400, "{\"error\":\"falta email\"}");
                    return;
                }
                p.pedidoId = "PED-" + UUID.randomUUID().toString().substring(0, 8);
                p.paymentStatus = "PENDING";
                p.creadoEn = Instant.now().toString();

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .deliveryMode(2)            // mensaje persistente
                        .build();

                // Un canal no es thread-safe: uno por petición
                try (Channel ch = conn.createChannel()) {
                    ch.basicPublish("", Config.Q_PEDIDOS, props,
                            GSON.toJson(p).getBytes(StandardCharsets.UTF_8));
                }
                Log.info(p.pedidoId, "Pedido recibido por API y publicado en " + Config.Q_PEDIDOS);
                responder(ex, 202, "{\"pedidoId\":\"" + p.pedidoId + "\",\"estado\":\"EN_COLA\"}");
            } catch (Exception e) {
                Log.info("-", "Error en /ordenar: " + e);
                responder(ex, 500, "{\"error\":\"" + e.getClass().getSimpleName() + "\"}");
            }
        });
        server.start();
        Log.info("-", "OrderProducer escuchando en http://localhost:8080/ordenar");
    }

    private static void responder(HttpExchange ex, int status, String json) throws IOException {
        byte[] out = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, out.length);
        ex.getResponseBody().write(out);
        ex.close();
    }
}
