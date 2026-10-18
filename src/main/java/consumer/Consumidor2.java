package consumer;

import com.rabbitmq.client.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.RabbitMQConfig;
import utility.Registo;

import java.util.Map;

public class Consumidor2 {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static int totalMensagensProcessadas = 0;
    
    public static void main(String[] args) {
        Registo.logInfo("=== SISTEMA CONSUMIDOR 2 INICIADO ===");
        
        try {
            ConnectionFactory factory = RabbitMQConfig.getConnectionFactory();
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            
            channel.exchangeDeclare(RabbitMQConfig.EXCHANGE_NAME, "direct", true);
            channel.queueDeclare(RabbitMQConfig.QUEUE_CONSUMIDOR2, true, false, false, null);
 
            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR2, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_2);
            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR2, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_ALL);
            
            Registo.logInfo("Consumidor 2 conectado ao RabbitMQ");
            Registo.logInfo("Aguardando mensagens das filas: " + RabbitMQConfig.QUEUE_CONSUMIDOR2);
            System.out.println("\n🔵 CONSUMIDOR 2 ATIVO - Pressione Ctrl+C para parar\n");
            
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String mensagemJson = new String(delivery.getBody(), "UTF-8");
                processarMensagem(mensagemJson, "Consumidor2");
            };
            
            channel.basicConsume(RabbitMQConfig.QUEUE_CONSUMIDOR2, true, deliverCallback, consumerTag -> {});

            while (true) {
                Thread.sleep(1000);
            }
            
        } catch (Exception e) {
            Registo.logErro("N/A", "Conexão Consumidor2", e.getMessage());
        }
    }
    
    private static void processarMensagem(String mensagemJson, String nomeConsumidor) {
        String messageId = "DESCONHECIDO";
        String conteudo = "N/A";
        String status = "SUCESSO";
        
        try {
       
            Map<String, Object> mensagem = objectMapper.readValue(mensagemJson, Map.class);
            messageId = (String) mensagem.get("id");
            conteudo = (String) mensagem.get("conteudo");
            String routingKey = (String) mensagem.get("routingKey");
 
            Thread.sleep(500 + (long)(Math.random() * 500));
            
            totalMensagensProcessadas++;
 
            String logConteudo = String.format("Conteúdo: \"%s\" | RoutingKey: %s | Por: %s", 
                                             conteudo, routingKey, nomeConsumidor);
            Registo.logRecebimento(messageId, logConteudo, status);

            System.out.printf(" [CONSUMIDOR2] Processada: %s | Total: %d\n", 
                            messageId, totalMensagensProcessadas);
            
        } catch (InterruptedException e) {
            status = "INTERROMPIDO";
            Registo.logErro(messageId, "Processamento Consumidor2", "Processamento interrompido");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            status = "ERRO";
            Registo.logErro(messageId, "Processamento Consumidor2", e.getMessage());
            Registo.logRecebimento(messageId, "ERRO: " + mensagemJson, status);
        }
    }
}