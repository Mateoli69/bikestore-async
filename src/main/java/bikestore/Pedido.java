package bikestore;

/** Mensaje JSON que viaja por el bróker. Campos públicos para que Gson lo serialice sin código extra. */
public class Pedido {
    public String pedidoId;
    public String cliente;
    public String email;
    public double total;
    public String paymentStatus;   // PENDING | PAID | FAILED
    public String creadoEn;
}
