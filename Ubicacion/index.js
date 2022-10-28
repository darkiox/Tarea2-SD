const express = require("express");
const app = express();
// var Jetty = require("jetty");
// var readline = require("readline")

const { Kafka } = require('kafkajs')
app.use(express.json());

const port = process.env.PORT;

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});
// var jetty = new Jetty(process.stdout);
// var rl = readline.createInterface({
//     input: process.stdin,
//     output: process.stdout,
//     terminal: true
//   });
console.clear();
const Ubicaciones = async () => {
    const consumer = kafka.consumer({ groupId: 'ubicacion', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ubicacion', partition: '0' });
    var daySaleArray = [];
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            if (message.value){
                var data = JSON.parse(message.value.toString());
                console.clear()
                console.log('Patente: '+data.patente+' | Ubicacion: '+data.ubicacion)
                // No funciono ninguno de los metodos para escribir persistentemente en la consola :(
            }
        },
    })
}
 
app.listen(port, () => {
    console.log(`Escuchando en puerto: ${port}`);
    Ubicaciones()
});