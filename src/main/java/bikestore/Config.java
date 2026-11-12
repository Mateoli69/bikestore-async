package bikestore;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;

/** Constantes y topología compartida por los tres procesos. */
public final class Config {

    public static final String Q_PEDIDOS    = "pedidos.pago";        // OrderProducer -> PaymentWorker
    public static final String Q_RESULTADOS = "pagos.resultado";     // PaymentWorker -> EmailWorker
    public static final String Q_DLQ        = "pedidos.pago.dlq";    // Dead-Letter Queue
    public static final String DLX          = "bikestore.dlx";       // Dead-Letter Exchange
    public static final String DLQ_KEY      = "pedidos.pago.dead";

    public static final int    MAX_REINTENTOS = 3;    // 1 intento inicial + 3 reintentos
    public static final double PROB_FALLO     = 0.5;  // 50 % de fallo simulado

    private Config() {}

    public static Connection conectar() throws Exception {
        ConnectionFactory f = new ConnectionFactory();   // guest/guest en localhost por defecto
        f.setHost(System.getenv().getOrDefault("RABBIT_HOST", "localhost"));
        return f.newConnection("bikestore");
    }

    /** Idempotente: los tres procesos la llaman al arrancar, con los mismos argumentos. */
    public static void declarar(Channel ch) throws IOException {
        ch.exchangeDeclare(DLX, BuiltinExchangeType.DIRECT, true);
        ch.queueDeclare(Q_DLQ, true, false, false, null);
        ch.queueBind(Q_DLQ, DLX, DLQ_KEY);

        // Si un mensaje es rechazado con requeue=false, RabbitMQ lo reenvía al DLX -> DLQ
        Map<String, Object> args = Map.<String, Object>of(
                "x-dead-letter-exchange", DLX,
                "x-dead-letter-routing-key", DLQ_KEY);
        ch.queueDeclare(Q_PEDIDOS, true, false, false, args);

        ch.queueDeclare(Q_RESULTADOS, true, false, false, null);
    }
}
