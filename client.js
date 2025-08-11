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
    const T1 = Date.now(); // Timestamp T1: Cuando el cliente MQTT envía el mensaje

    const message = {
        messageType: 'latency_request',  // Identificador para mensajes de latencia
        messageId: messageId,            // ID único del mensaje
        timestamp: T1,                   // Timestamp de envío
        timestamps: {                    // Objeto para almacenar todos los timestamps
            T1: T1,
        },
        data: variables                  // Datos de las variables
    };

    // Guardar timestamp de envío
    sentMessages.set(messageId, {
        sendTime: T1,
        responseReceived: false
    });

    // Publicar mensaje
    mqttClient.publish(process.env.MQTT_TOPIC, JSON.stringify(message));
    console.log(`Mensaje de latencia enviado - ID: ${messageId}, T1: ${T1}`);
    console.log('Datos enviados:', variables);
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
        
        if (data.messageType === 'latency_response') {
                // Si recibimos una respuesta a uno de nuestros mensajes
                if (sentMessages.has(data.originalMessageId)) {

                    const T4 = Date.now(); // Timestamp T4: Cuando el cliente MQTT recibe la respuesta
                    const messageInfo = sentMessages.get(data.originalMessageId);


                    // Asegurarnos de que timestamps existe
                    const timestamps = data.timestamps || {};
                
                    console.log(`Timestamps recibidos en respuesta:`, timestamps);  // Debug log
                    
                    // Calcular RTT total y parciales
                    const totalRTT = T4 - timestamps.T1;
                    const mqttToWebSocket = timestamps.T2 - timestamps.T1;
                    const webSocketToFrontend = timestamps.T3 - timestamps.T2;
                    const frontendToMQTT = T4 - timestamps.T3;

                    console.log(`Latencia para mensaje ${data.originalMessageId}:`);
                    console.log(`T1 (Cliente MQTT envío): ${timestamps.T1}`);
                    console.log(`T2 (${data.websocketType} recepción): ${timestamps.T2}`);
                    if (timestamps.T2_5) {
                        const djangoProcessing = timestamps.T2_5 - timestamps.T2;
                        const djangoToFrontend = timestamps.T3 - timestamps.T2_5;
                        console.log(`T2.5 (Django procesamiento): ${timestamps.T2_5}`);
                        console.log(`Procesamiento en Django: ${djangoProcessing}ms`);
                        console.log(`Django -> Frontend: ${djangoToFrontend}ms`);
                    }
                    console.log(`T3 (Frontend recepción): ${timestamps.T3}`);
                    console.log(`T4 (Cliente MQTT recepción): ${T4}`);
                    console.log(`RTT Total: ${totalRTT}ms`);
                    console.log(`MQTT -> WebSocket: ${mqttToWebSocket}ms`);
                    console.log(`WebSocket -> Frontend: ${webSocketToFrontend}ms`);
                    console.log(`Frontend -> MQTT: ${frontendToMQTT}ms`);
                        
                    // Limpiar el mensaje del Map
                    sentMessages.delete(data.originalMessageId);
                }
        }
        else {
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