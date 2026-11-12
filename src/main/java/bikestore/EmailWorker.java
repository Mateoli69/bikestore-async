package bikestore;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

/** Envía (simulado) el e-mail de confirmación solo si paymentStatus = PAID. */
public class EmailWorker {

    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws Exception {
        Connection conn = Config.conectar();
        Channel ch = conn.createChannel();
        Config.declarar(ch);
        ch.basicQos(1);

        DeliverCallback callback = (tag, delivery) -> {
            try {
                Pedido p = GSON.fromJson(new String(delivery.getBody(), StandardCharsets.UTF_8), Pedido.class);
                if ("PAID".equals(p.paymentStatus)) {
                    Thread.sleep(300);   // simula el envío SMTP
                    Log.info(p.pedidoId, "E-mail de confirmación ENVIADO a " + p.email
                            + " (total $" + p.total + ")");
                } else {
                    Log.info(p.pedidoId, "E-mail OMITIDO: paymentStatus=" + p.paymentStatus);
                }
                ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                Log.info("-", "Error enviando e-mail: " + e);
                ch.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        };

        ch.basicConsume(Config.Q_RESULTADOS, false, callback, tag -> { });
        Log.info("-", "EmailWorker esperando eventos en " + Config.Q_RESULTADOS);
    }
}
