package bikestore;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Simula el cobro con tarjeta (50 % de fallo).
 *  - Éxito   -> publica evento PAID en pagos.resultado y hace ack.
 *  - Fallo   -> reintenta hasta 3 veces (reencola con header x-intentos y espera creciente).
 *  - Agotado -> publica evento FAILED, hace nack(requeue=false) y RabbitMQ lo manda a la DLQ.
 */
public class PaymentWorker {

    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws Exception {
        Connection conn = Config.conectar();
        Channel ch = conn.createChannel();
        Config.declarar(ch);
        ch.basicQos(1);   // un mensaje a la vez por worker

        DeliverCallback callback = (tag, delivery) -> {
            try {
                procesar(ch, delivery);
            } catch (Exception e) {
                Log.info("-", "Error inesperado, mensaje a DLQ: " + e);
                ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            }
        };

        ch.basicConsume(Config.Q_PEDIDOS, false, callback, tag -> { });   // autoAck = false
        Log.info("-", "PaymentWorker esperando pedidos en " + Config.Q_PEDIDOS);
    }

    private static void procesar(Channel ch, Delivery d) throws Exception {
        long tag = d.getEnvelope().getDeliveryTag();
        Pedido p = GSON.fromJson(new String(d.getBody(), StandardCharsets.UTF_8), Pedido.class);

        Map<String, Object> headers = d.getProperties().getHeaders();
        int intento = (headers != null && headers.get("x-intentos") instanceof Number n) ? n.intValue() + 1 : 1;

        Log.info(p.pedidoId, "Procesando pago (intento " + intento + " de " + (Config.MAX_REINTENTOS + 1) + ")");
        boolean aprobado = Math.random() >= Config.PROB_FALLO;

        if (aprobado) {
            p.paymentStatus = "PAID";
            publicarResultado(ch, p);
            ch.basicAck(tag, false);
            Log.info(p.pedidoId, "Pago APROBADO -> evento publicado en " + Config.Q_RESULTADOS);

        } else if (intento <= Config.MAX_REINTENTOS) {
            Log.info(p.pedidoId, "Pago RECHAZADO -> reintento " + intento + " de " + Config.MAX_REINTENTOS);
            Thread.sleep(1000L * intento);   // espera creciente entre reintentos
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .headers(Map.<String, Object>of("x-intentos", intento))
                    .build();
            ch.basicPublish("", Config.Q_PEDIDOS, props, d.getBody());
            ch.basicAck(tag, false);

        } else {
            p.paymentStatus = "FAILED";
            publicarResultado(ch, p);
            ch.basicNack(tag, false, false);   // requeue=false -> DLX -> DLQ
            Log.info(p.pedidoId, "Pago FALLIDO tras " + Config.MAX_REINTENTOS
                    + " reintentos -> enviado a DLQ (" + Config.Q_DLQ + ")");
        }
    }

    private static void publicarResultado(Channel ch, Pedido p) throws Exception {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json").deliveryMode(2).build();
        ch.basicPublish("", Config.Q_RESULTADOS, props, GSON.toJson(p).getBytes(StandardCharsets.UTF_8));
    }
}
