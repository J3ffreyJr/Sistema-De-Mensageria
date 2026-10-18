package messaging;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitTopologyTest {

    @Test
    void declaresDurableRoutingRetryAndDeadLetterTopology() {
        List<String> calls = new ArrayList<>();
        Channel channel = recordingChannel(calls);

        new RabbitTopology().declare(channel);

        assertTrue(calls.contains("exchangeDeclare:exchange_direto:direct:true"));
        assertTrue(calls.contains("queueDeclare:fila_consumidor1:true"));
        assertTrue(calls.contains("queueDeclare:fila_consumidor2:true"));
        assertTrue(calls.contains("queueBind:fila_consumidor1:consumidor1"));
        assertTrue(calls.contains("queueBind:fila_consumidor1:ambos"));
        assertTrue(calls.contains("queueBind:fila_consumidor2:consumidor2"));
        assertTrue(calls.contains("queueBind:fila_consumidor2:ambos"));
        assertTrue(calls.contains("queueDeclare:fila_consumidor1_retry:true"));
        assertTrue(calls.contains("queueDeclare:fila_consumidor2_retry:true"));
        assertTrue(calls.contains("queueDeclare:fila_consumidor1_dead_letter:true"));
        assertTrue(calls.contains("queueDeclare:fila_consumidor2_dead_letter:true"));
        assertTrue(calls.contains("queueBind:fila_consumidor1_dead_letter:consumidor1"));
        assertTrue(calls.contains("queueBind:fila_consumidor2_dead_letter:consumidor2"));
        assertEquals(3, calls.stream().filter(call -> call.startsWith("exchangeDeclare:exchange_direto")).count());
    }

    private static Channel recordingChannel(List<String> calls) {
        return (Channel) Proxy.newProxyInstance(
                Channel.class.getClassLoader(),
                new Class<?>[]{Channel.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("exchangeDeclare")) {
                        calls.add("exchangeDeclare:" + args[0] + ":" + args[1] + ":" + args[2]);
                    } else if (method.getName().equals("queueDeclare")) {
                        calls.add("queueDeclare:" + args[0] + ":" + args[1]);
                    } else if (method.getName().equals("queueBind")) {
                        calls.add("queueBind:" + args[0] + ":" + args[2]);
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return type == Map.class ? Map.of() : null;
        }
        if (type == boolean.class) return false;
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0F;
        if (type == double.class) return 0D;
        if (type == char.class) return '\0';
        return null;
    }
}
