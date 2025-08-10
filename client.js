import * as mqtt from 'mqtt';
import dotenv from 'dotenv';

// Configurar dotenv para variables de entorno
dotenv.config();

// Configuración del cliente MQTT
const mqttOptions = {
    host: process.env.MQTT_BROKER,
    port: process.env.MQTT_PORT,
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    protocol: 'mqtts',
    rejectUnauthorized: true
};

// Creación del cliente MQTT
const mqttClient = mqtt.connect(mqttOptions);

// Almacenar timestamps de mensajes enviados
const sentMessages = new Map();

// Función para generar un ID único para cada mensaje
function generateMessageId() {
    return `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Función para enviar mensaje de latencia
function sendLatencyMessage(variables) {
    
    const messageId = generateMessageId();
    const message = {
        messageType: 'latency_request',  // Identificador para mensajes de latencia
        messageId: messageId,            // ID único del mensaje
        timestamp: Date.now(),           // Timestamp de envío
        data: variables                  // Datos de las variables
    };

    // Guardar timestamp de envío
    sentMessages.set(messageId, {
        sendTime: Date.now(),
        responseReceived: false
    });

    // Publicar mensaje
    mqttClient.publish(process.env.MQTT_TOPIC, JSON.stringify(message));
    console.log(`Mensaje de latencia enviado - ID: ${messageId}`);
    console.log('Datos enviados:', variables);
}


// Función para enviar respuesta a un mensaje de latencia
function sendLatencyResponse(originalMessage, websocketType) {
    const responseMessage = {
        messageType: 'latency_response',     // Identificador para respuestas
        originalMessageId: originalMessage.messageId,  // ID del mensaje original
        timestamp: Date.now(),               // Timestamp de la respuesta
        data: originalMessage.data,          // Devolver los mismos datos
        websocketType: websocketType         // Tipo de WebSocket que respondió
    };

    mqttClient.publish(process.env.MQTT_TOPIC, JSON.stringify(responseMessage));
    console.log(`Respuesta de latencia enviada para mensaje: ${originalMessage.messageId} desde ${websocketType}`);
}


// Manejo de eventos del cliente MQTT
mqttClient.on('connect', () => {
    console.log('Conectado al broker MQTT');
    
    // Suscribirse al tópico
    mqttClient.subscribe(process.env.MQTT_TOPIC, (err) => {
        if (err) {
            console.error('Error al suscribirse al tópico:', err);
        } else {
            console.log(`Suscrito al tópico: ${process.env.MQTT_TOPIC}`);
        }
    });

    // Ejemplo: enviar un mensaje cada 5 segundos
    setInterval(() => {
        const testData = {
            "1": 1,
            "2": 0
        };
        sendLatencyMessage(testData);
    }, 5000);
});

// Manejo de mensajes recibidos
mqttClient.on('message', (topic, message) => {
    try {
        const data = JSON.parse(message.toString());
        
        switch(data.messageType) {
            case 'latency_request':
                // Si recibimos un mensaje de latencia, enviamos una respuesta
                // (pero no a nuestros propios mensajes)
                if (!sentMessages.has(data.messageId)) {
                    sendLatencyResponse(data);
                }
                break;

            case 'latency_response':
                // Si recibimos una respuesta a uno de nuestros mensajes
                if (sentMessages.has(data.originalMessageId)) {
                    const messageInfo = sentMessages.get(data.originalMessageId);
                    const receiveTime = Date.now();
                    const rtt = receiveTime - messageInfo.sendTime;

                    console.log(`RTT completo para mensaje ${data.originalMessageId}: ${rtt}ms`);
                    console.log(`WebSocket que respondió: ${data.websocketType}`);
                    console.log('Datos recibidos:', data.data);
                    
                    // Limpiar el mensaje del Map
                    sentMessages.delete(data.originalMessageId);
                }
                break;

            default:
                console.log('Mensaje recibido de tipo desconocido:', data);
        }
    } catch (error) {
        console.error('Error al procesar mensaje:', error);
    }
});


// Manejo de errores
mqttClient.on('error', (error) => {
    console.error('Error en la conexión MQTT:', error);
});

// Manejo del cierre del cliente
process.on('SIGINT', () => {
    console.log('Cerrando cliente MQTT...');
    
    mqttClient.end(false, () => {
        console.log('Cliente MQTT desconectado');
        process.exit(0);
    });
});

// Limpieza periódica de mensajes antiguos (timeout después de 30 segundos)
setInterval(() => {
    const now = Date.now();
    for (const [messageId, messageInfo] of sentMessages.entries()) {
        if (now - messageInfo.sendTime > 30000) { // 30 segundos
            console.log(`Mensaje ${messageId} expirado sin respuesta`);
            sentMessages.delete(messageId);
        }
    }
}, 5000);