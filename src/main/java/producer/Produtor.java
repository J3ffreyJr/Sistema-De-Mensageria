package producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.RabbitMQConfig;
import utility.Registo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

public class Produtor {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
    
    public static void main(String[] args) {
        Registo.iniciarNovoLog();
        Registo.logInfo("=== SISTEMA PRODUTOR COM ROTEAMENTO INICIADO ===");
        
        Connection connection = null;
        Channel channel = null;
        
        try {
            
            ConnectionFactory factory = RabbitMQConfig.getConnectionFactory();
            connection = factory.newConnection();
            channel = connection.createChannel();

            channel.exchangeDeclare(RabbitMQConfig.EXCHANGE_NAME, "direct", true);

            channel.queueDeclare(RabbitMQConfig.QUEUE_CONSUMIDOR1, true, false, false, null);
            channel.queueDeclare(RabbitMQConfig.QUEUE_CONSUMIDOR2, true, false, false, null);

            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR1, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_1);
            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR2, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_2);
          
            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR1, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_ALL);
            channel.queueBind(RabbitMQConfig.QUEUE_CONSUMIDOR2, RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY_ALL);
            
            Registo.logInfo("Conectado ao RabbitMQ com sucesso");
            Registo.logInfo("Exchange: " + RabbitMQConfig.EXCHANGE_NAME);
            Registo.logInfo("Filas: " + RabbitMQConfig.QUEUE_CONSUMIDOR1 + ", " + RabbitMQConfig.QUEUE_CONSUMIDOR2);
            
            System.out.println("\n=== OPÇÕES DE DESTINO ===");
            System.out.println("1 - Apenas Consumidor 1");
            System.out.println("2 - Apenas Consumidor 2"); 
            System.out.println("3 - Ambos os Consumidores");
            System.out.println("sair - Encerrar");
            System.out.println("\nDigite o destino seguido da mensagem (ex: '1 Olá mundo'):");
            
            Scanner scanner = new Scanner(System.in);
            String input;
            int contador = 1;
            
            while (true) {
                System.out.print("Mensagem " + contador + ": ");
                input = scanner.nextLine();
                
                if ("sair".equalsIgnoreCase(input)) {
                    break;
                }
                
                if (!input.trim().isEmpty()) {
                    processarInput(channel, input, contador);
                    contador++;
                } else {
                    System.out.println("Entrada vazia, tente novamente.");
                }
            }
            
            scanner.close();
            Registo.logInfo("Produtor encerrado. Total de mensagens enviadas: " + (contador - 1));
            
        } catch (Exception e) {
            Registo.logErro("N/A", "Conexão Produtor", e.getMessage());
        } finally {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
                if (connection != null && connection.isOpen()) {
                    connection.close();
                }
            } catch (Exception e) {
                Registo.logErro("N/A", "Fechamento Conexão", e.getMessage());
            }
        }
    }
    
    private static void processarInput(Channel channel, String input, int numeroMensagem) {
        try {
            String[] partes = input.split(" ", 2);
            if (partes.length < 2) {
                System.out.println("Formato inválido. Use: 'destino mensagem'");
                return;
            }
            
            String destino = partes[0];
            String conteudo = partes[1];
            String routingKey;
            String destinatarios;
            
            switch (destino) {
                case "1":
                    routingKey = RabbitMQConfig.ROUTING_KEY_1;
                    destinatarios = "Consumidor1";
                    break;
                case "2":
                    routingKey = RabbitMQConfig.ROUTING_KEY_2;
                    destinatarios = "Consumidor2";
                    break;
                case "3":
                    routingKey = RabbitMQConfig.ROUTING_KEY_ALL;
                    destinatarios = "AmbosConsumidores";
                    break;
                default:
                    System.out.println("Destino inválido. Use 1, 2 ou 3.");
                    return;
            }
            
            enviarMensagem(channel, conteudo, numeroMensagem, routingKey, destinatarios);
            
        } catch (Exception e) {
            Registo.logErro("N/A", "Processamento Input", e.getMessage());
        }
    }
    
    private static void enviarMensagem(Channel channel, String conteudo, int numeroMensagem, 
                                     String routingKey, String destinatarios) throws Exception {
        String messageId = "PROD-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8);
        
        try {
           
            Map<String, Object> mensagemObj = new HashMap<>();
            mensagemObj.put("id", messageId);
            mensagemObj.put("conteudo", conteudo);
            mensagemObj.put("timestamp", LocalDateTime.now().format(formatter));
            mensagemObj.put("produtor", "ProdutorJava");
            mensagemObj.put("numero", numeroMensagem);
            mensagemObj.put("routingKey", routingKey);
            mensagemObj.put("destinatarios", destinatarios);
            
            String mensagemJson = objectMapper.writeValueAsString(mensagemObj);
       
            channel.basicPublish(RabbitMQConfig.EXCHANGE_NAME, routingKey, null, mensagemJson.getBytes());
        
            String logConteudo = String.format("Conteúdo: \"%s\" | Destino: %s", conteudo, destinatarios);
            Registo.logEnvio(messageId, logConteudo);
            
        } catch (Exception e) {
            Registo.logErro(messageId, "Envio Mensagem", e.getMessage());
            throw e;
        }
    }
}