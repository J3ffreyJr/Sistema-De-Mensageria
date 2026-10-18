package consumer;

import com.rabbitmq.client.*;
import config.RabbitMQConfig;
import messaging.Message;
import messaging.MessageCodec;
import utility.Registo;

public class Consumidor1 {
    
    private static final MessageCodec messageCodec = new MessageCodec();
    private static int totalMensagensProcessadas = 0;
    
    public static void main(String[] args) {
        Registo.logInfo("=== SISTEMA CONSUMIDOR 1 INICIADO ===");
        
        try {
            ConnectionFactory factory = RabbitMQConfig.getConnectionFactory();
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
 
            channel.exchangeDeclare(RabbitMQConfig.EXCHANGE_NAME, "direct", true);
            channel.queueDeclare(RabbitMQConfig.QUEUE_CONSUMIDOR1, true, false, false, null);

            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR1, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_1);
            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR1, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_ALL);
            
            Registo.logInfo("Consumidor 1 conectado ao RabbitMQ");
            Registo.logInfo("Aguardando mensagens das filas: " + RabbitMQConfig.QUEUE_CONSUMIDOR1);
            System.out.println("\n🟢 CONSUMIDOR 1 ATIVO - Pressione Ctrl+C para parar\n");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                processarMensagem(delivery.getBody(), "Consumidor1");
            };
  
            channel.basicConsume(RabbitMQConfig.QUEUE_CONSUMIDOR1, true, deliverCallback, consumerTag -> {});
     
            while (true) {
                Thread.sleep(1000);
            }
            
        } catch (Exception e) {
            Registo.logErro("N/A", "Conexão Consumidor1", e.getMessage());
        }
    }
    
    private static void processarMensagem(byte[] payload, String nomeConsumidor) {
        String messageId = "DESCONHECIDO";
        String conteudo = "N/A";
        String status = "SUCESSO";
        
        try {
           
            Message mensagem = messageCodec.decode(payload);
            messageId = mensagem.messageId();
            conteudo = mensagem.content();
            String routingKey = mensagem.routingKey();
         
            Thread.sleep(1000 + (long)(Math.random() * 500));
            
            totalMensagensProcessadas++;
  
            String logConteudo = String.format("Conteúdo: \"%s\" | RoutingKey: %s | Por: %s", 
                                             conteudo, routingKey, nomeConsumidor);
            Registo.logRecebimento(messageId, logConteudo, status);
      
            System.out.printf(" [CONSUMIDOR1] Processada: %s | Total: %d\n", 
                            messageId, totalMensagensProcessadas);
            
        } catch (InterruptedException e) {
            status = "INTERROMPIDO";
            Registo.logErro(messageId, "Processamento Consumidor1", "Processamento interrompido");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            status = "ERRO";
            Registo.logErro(messageId, "Processamento Consumidor1", e.getMessage());
            Registo.logRecebimento(messageId, "ERRO: payload inválido", status);
        }
    }
}