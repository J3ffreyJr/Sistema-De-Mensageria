package producer;

import config.RabbitMQProperties;
import messaging.Message;
import messaging.PublishResult;
import messaging.RabbitPublisher;
import config.RabbitMQConfig;
import utility.Registo;

import java.time.Instant;
import java.util.Scanner;
import java.util.UUID;

public class Produtor {
    
    public static void main(String[] args) {
        Registo.iniciarNovoLog();
        Registo.logInfo("=== SISTEMA PRODUTOR COM ROTEAMENTO INICIADO ===");
        
        try (RabbitPublisher publisher = new RabbitPublisher(RabbitMQProperties.fromEnvironment(System.getenv()))) {
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
                    processarInput(publisher, input, contador);
                    contador++;
                } else {
                    System.out.println("Entrada vazia, tente novamente.");
                }
            }
            
            scanner.close();
            Registo.logInfo("Produtor encerrado. Total de mensagens enviadas: " + (contador - 1));
            
        } catch (Exception e) {
            Registo.logErro("N/A", "Conexão Produtor", e.getMessage());
        }
    }
    
    private static void processarInput(RabbitPublisher publisher, String input, int numeroMensagem) {
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
            
            enviarMensagem(publisher, conteudo, numeroMensagem, routingKey, destinatarios);
            
        } catch (Exception e) {
            Registo.logErro("N/A", "Processamento Input", e.getMessage());
        }
    }
    
    private static void enviarMensagem(RabbitPublisher publisher, String conteudo, int numeroMensagem, 
                                     String routingKey, String destinatarios) throws Exception {
        String messageId = "PROD-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8);
        
        try {
            Message message = new Message(1, messageId, conteudo, Instant.now(), "ProdutorJava", routingKey);
            PublishResult result = publisher.publish(message);
            if (result.status() != PublishResult.Status.CONFIRMED) {
                throw new IllegalStateException("Mensagem não confirmada: " + result.reason());
            }
        
            String logConteudo = String.format("Conteúdo: \"%s\" | Destino: %s", conteudo, destinatarios);
            Registo.logEnvio(messageId, logConteudo);
            
        } catch (Exception e) {
            Registo.logErro(messageId, "Envio Mensagem", e.getMessage());
            throw e;
        }
    }
}