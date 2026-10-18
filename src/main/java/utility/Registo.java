package utility;


import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Registo {
    private static final String LOG_FILE = "logs_mensageria.txt";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
    
    public static void logEnvio(String messageId, String conteudo) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format(
            "ENVIO     | %s | ID: %s | Conteúdo: \"%s\"\n",
            timestamp, messageId, conteudo
        );
        writeToFile(logEntry);
        System.out.println("✅ [ENVIADO] " + logEntry.trim());
    }
    
    public static void logRecebimento(String messageId, String conteudo, String status) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format(
            "RECEBIDO  | %s | ID: %s | Status: %s | Conteúdo: \"%s\"\n",
            timestamp, messageId, status, conteudo
        );
        writeToFile(logEntry);
        System.out.println("📥 [RECEBIDO] " + logEntry.trim());
    }
    
    public static void logErro(String messageId, String operacao, String erro) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format(
            "ERRO      | %s | Operação: %s | ID: %s | Erro: %s\n",
            timestamp, operacao, messageId, erro
        );
        writeToFile(logEntry);
        System.err.println("❌ [ERRO] " + logEntry.trim());
    }
    
    public static void logInfo(String mensagem) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format("INFO      | %s | %s\n", timestamp, mensagem);
        writeToFile(logEntry);
        System.out.println("ℹ️  [INFO] " + mensagem);
    }
    
    private static void writeToFile(String content) {
        try (FileWriter writer = new FileWriter(LOG_FILE, true);
             PrintWriter pw = new PrintWriter(writer)) {
            pw.print(content);
            pw.flush();
        } catch (IOException e) {
            System.err.println("Falha ao escrever no ficheiro de log: " + e.getMessage());
        }
    }
    
    // Método para iniciar novo arquivo de log
    public static void iniciarNovoLog() {
        try (FileWriter writer = new FileWriter(LOG_FILE, false);
             PrintWriter pw = new PrintWriter(writer)) {
            String header = "=== SISTEMA DE MENSAGERIA - REGISTO DE MENSAGENS ===\n";
            header += "Iniciado em: " + LocalDateTime.now().format(formatter) + "\n";
            header += "=".repeat(60) + "\n";
            pw.print(header);
        } catch (IOException e) {
            System.err.println("Falha ao criar ficheiro de log: " + e.getMessage());
        }
    }
}