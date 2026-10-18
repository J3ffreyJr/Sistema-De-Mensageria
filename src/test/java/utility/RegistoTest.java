package utility;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistoTest {

    private static final Path LOG_FILE = Path.of("logs_mensageria.txt");
    private byte[] originalLog;

    @BeforeEach
    void preserveExistingLogFile() throws Exception {
        originalLog = Files.exists(LOG_FILE) ? Files.readAllBytes(LOG_FILE) : null;
        Files.deleteIfExists(LOG_FILE);
    }

    @AfterEach
    void restoreExistingLogFile() throws Exception {
        if (originalLog == null) {
            Files.deleteIfExists(LOG_FILE);
        } else {
            Files.write(LOG_FILE, originalLog);
        }
    }

    @Test
    void iniciaNovoLogEscreveCabecalho() throws Exception {
        Registo.iniciarNovoLog();

        String content = Files.readString(LOG_FILE);

        assertTrue(content.startsWith("=== SISTEMA DE MENSAGERIA - REGISTO DE MENSAGENS ==="));
        assertTrue(content.contains("Iniciado em: "));
        assertTrue(content.contains("============================================================"));
    }

    @Test
    void registaEnvioRecebimentoEInformacao() throws Exception {
        Registo.logEnvio("msg-1", "hello");
        Registo.logRecebimento("msg-1", "hello", "SUCESSO");
        Registo.logInfo("ready");

        String content = Files.readString(LOG_FILE);

        assertTrue(content.contains("ENVIO     | "));
        assertTrue(content.contains("ID: msg-1 | Conteúdo: \"hello\""));
        assertTrue(content.contains("RECEBIDO  | "));
        assertTrue(content.contains("Status: SUCESSO"));
        assertTrue(content.contains("INFO      | "));
        assertTrue(content.contains("ready"));
    }

    @Test
    void registaErroComOperacaoEMensagem() throws Exception {
        Registo.logErro("msg-2", "publish", "connection failed");

        String content = Files.readString(LOG_FILE);

        assertTrue(content.contains("ERRO      | "));
        assertTrue(content.contains("Operação: publish | ID: msg-2 | Erro: connection failed"));
    }
}