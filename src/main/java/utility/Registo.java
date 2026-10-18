package utility;

/** Compatibility facade retained for callers not yet migrated to MessageLog. */
public final class Registo {
    private Registo() { }

    public static void logEnvio(String messageId, String ignoredContent) {
        MessageLog.publish(messageId, null, "unknown");
    }

    public static void logRecebimento(String messageId, String ignoredContent, String status) {
        MessageLog.receiveStatus(messageId, status);
    }

    public static void logErro(String messageId, String operacao, String erro) {
        MessageLog.publishError(messageId, operacao, erro);
    }

    public static void logInfo(String mensagem) {
        MessageLog.info(mensagem);
    }

    public static void iniciarNovoLog() {
        MessageLog.connection("started", "compatibility facade");
    }
}
