import * as mqtt from 'mqtt';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

// Configurar dotenv para variables de entorno
dotenv.config();

// Configuración para el análisis de latencia
const TOTAL_MESSAGES = 1005;
const WARMUP_MESSAGES = 5;
const INTERVAL_MS = 2500;
let messageCount = 0;


//Configuración Nivel QoS
const QOS_LEVEL = process.env.MQTT_QOS ? parseInt(process.env.MQTT_QOS) : 0; //QoS 0 por defecto
console.log(`Usando QoS nivel: ${QOS_LEVEL}`);

// Estructura para almacenar resultados
const latencyResults = [];

// Función para guardar resultados en CSV
function saveToCSV(results, implementation) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `latency_results_${implementation}_${timestamp}.csv`;
    
    // Crear encabezados del CSV
    const headers = [
        'messageId',
        'T1',
        'T2',
        'T2_5',
        'T3',
        'T4',
        'RTT_Total',
        'MQTT_to_WebSocket',
        'WebSocket_Processing',
        'Django_Processing',
        'Django_to_Frontend',
        'Frontend_to_MQTT'
    ].join(',');

    // Crear contenido del CSV
    const csvContent = results.map(result => [
        result.messageId,
        result.timestamps.T1,
        result.timestamps.T2,
        result.timestamps.T2_5 || '',
        result.timestamps.T3,
        result.timestamps.T4,
        result.latencies.totalRTT,
        result.latencies.mqttToWebSocket,
        result.latencies.webSocketProcessing,
        result.latencies.djangoProcessing || '',
        result.latencies.djangoToFrontend || '',
        result.latencies.frontendToMQTT
    ].join(',')).join('\n');

    // Escribir archivo
    fs.writeFileSync(filename, `${headers}\n${csvContent}`);
    console.log(`Resultados guardados en: ${filename}`);
}

// Función para calcular estadísticas
function calculateStatistics(results) {
    const metrics = ['totalRTT', 'mqttToWebSocket', 'webSocketProcessing', 'djangoProcessing', 'djangoToFrontend', 'frontendToMQTT'];
    const stats = {};

    metrics.forEach(metric => {
        const values = results.map(r => r.latencies[metric]).filter(v => v !== undefined);
        if (values.length > 0) {
            // Calcular estadísticas básicas
            const sum = values.reduce((a, b) => a + b, 0);
            const mean = sum / values.length;
            const sortedValues = [...values].sort((a, b) => a - b);
            const median = sortedValues[Math.floor(values.length / 2)];
            
            // Calcular desviación estándar
            const squareDiffs = values.map(value => Math.pow(value - mean, 2));
            const avgSquareDiff = squareDiffs.reduce((a, b) => a + b, 0) / values.length;
            const stdDev = Math.sqrt(avgSquareDiff);

            // Calcular percentiles
            const p95 = sortedValues[Math.floor(values.length * 0.95)];
            const p99 = sortedValues[Math.floor(values.length * 0.99)];

            stats[metric] = {
                min: Math.min(...values),
                max: Math.max(...values),
                mean: mean,
                median: median,
                stdDev: stdDev,
                p95: p95,
                p99: p99
            };
        }
    });

    return stats;
}

// Función para imprimir estadísticas
function printStatistics(stats) {
    console.log('\n=== Estadísticas de Latencia ===');
    Object.entries(stats).forEach(([metric, values]) => {
        console.log(`\n${metric}:`);
        console.log(`  Mínimo: ${values.min.toFixed(2)}ms`);
        console.log(`  Máximo: ${values.max.toFixed(2)}ms`);
        console.log(`  Media: ${values.mean.toFixed(2)}ms`);
        console.log(`  Mediana: ${values.median.toFixed(2)}ms`);
        console.log(`  Desviación Estándar: ${values.stdDev.toFixed(2)}ms`);
        console.log(`  Percentil 95: ${values.p95.toFixed(2)}ms`);
        console.log(`  Percentil 99: ${values.p99.toFixed(2)}ms`);
    });
}

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

    if (messageCount >= TOTAL_MESSAGES) {
        // Calcular y mostrar estadísticas finales
        const stats = calculateStatistics(latencyResults.slice(WARMUP_MESSAGES));
        printStatistics(stats);
        
        // Guardar resultados en CSV
        saveToCSV(latencyResults.slice(WARMUP_MESSAGES), process.env.WS_TYPE || 'unknown');
        
        // Terminar el proceso
        console.log('\nPrueba de latencia completada. Cerrando cliente...');
        mqttClient.end(false, () => process.exit(0));
        return;
    }

    messageCount++;

    const messageId = generateMessageId(); // Generar un ID único para el mensaje
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

    // Publicar mensaje con QoS configurable
    mqttClient.publish(
        process.env.MQTT_TOPIC, 
        JSON.stringify(message),
        { qos: QOS_LEVEL },
        (error) => {
            if (error) {
                console.error(`Error al publicar mensaje ${messageId}:`, error);
            } else {
                console.log(`Mensaje ${messageCount}/${TOTAL_MESSAGES} enviado - ID: ${messageId}, T1: ${T1}, QoS: ${QOS_LEVEL}`);
            }
        }
    );
}


// Manejo de eventos del cliente MQTT
mqttClient.on('connect', () => {
    console.log('Conectado al broker MQTT');
    
    // Suscribirse al tópico
    mqttClient.subscribe(process.env.MQTT_TOPIC, { qos: QOS_LEVEL }, (err) => {
        if (err) {
            console.error('Error al suscribirse al tópico:', err);
        } else {
            console.log(`Suscrito al tópico: ${process.env.MQTT_TOPIC}`);
            console.log(`\nIniciando prueba de latencia:`);
            console.log(`- Total de mensajes: ${TOTAL_MESSAGES}`);
            console.log(`- Mensajes de calentamiento: ${WARMUP_MESSAGES}`);
            console.log(`- Intervalo entre mensajes: ${INTERVAL_MS}ms\n`);

            // Iniciar envío de mensajes
            const interval = setInterval(() => {
                const testData = {
                    "1": 1,
                    "2": 0
                };
                sendLatencyMessage(testData);
            }, INTERVAL_MS);

            // Limpiar intervalo cuando se complete
            if (messageCount >= TOTAL_MESSAGES) {
                clearInterval(interval);
            }
        }
    });
});

// Manejo de mensajes recibidos
mqttClient.on('message', (topic, message) => {
    try {
        const data = JSON.parse(message.toString());
        
        if (data.messageType === 'latency_response') {
            if (sentMessages.has(data.originalMessageId)) {
                const T4 = Date.now();
                const timestamps = {
                    ...data.timestamps,
                    T4: T4
                };

                // Calcular latencias
                const latencies = {
                    totalRTT: T4 - timestamps.T1,
                    mqttToWebSocket: timestamps.T2 - timestamps.T1,
                    webSocketProcessing: timestamps.T2_5 ? timestamps.T2_5 - timestamps.T2 : undefined,
                    djangoProcessing: timestamps.T2_5 ? timestamps.T2_5 - timestamps.T2 : undefined,
                    djangoToFrontend: timestamps.T2_5 ? timestamps.T3 - timestamps.T2_5 : timestamps.T3 - timestamps.T2,
                    frontendToMQTT: T4 - timestamps.T3
                };

                // Guardar resultados
                latencyResults.push({
                    messageId: data.originalMessageId,
                    timestamps: timestamps,
                    latencies: latencies
                });

                // Imprimir resultados individuales
                console.log(`\nResultados para mensaje ${messageCount}:`);
                console.log(`RTT Total: ${latencies.totalRTT}ms`);
                console.log(`MQTT -> WebSocket: ${latencies.mqttToWebSocket}ms`);
                if (timestamps.T2_5) {
                    console.log(`Procesamiento en Django: ${latencies.djangoProcessing}ms`);
                    console.log(`Django -> Frontend: ${latencies.djangoToFrontend}ms`);
                }
                console.log(`Frontend -> MQTT: ${latencies.frontendToMQTT}ms`);

                sentMessages.delete(data.originalMessageId);
            }
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