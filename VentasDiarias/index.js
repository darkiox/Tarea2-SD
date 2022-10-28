const express = require("express");
const app = express();

const { Kafka } = require('kafkajs')
app.use(express.json());

const port = process.env.PORT;

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});
ventasDailyArray = [];
const ventasDiarias = async () => {
    const consumer = kafka.consumer({ groupId: 'ventas', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ventas' });
    var daySaleArray = [];
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            if (message.value){
                var data = JSON.parse(message.value.toString());
                daySaleArray.push(message.value.toString())
                var ayer = new Date(); // Ejemplo, hoy es 28
                ayer.setDate(ayer.getDate()-1); // 27
                ayer = ayer.toLocaleDateString("es-CL");
                ventasDailyArray = daySaleArray; // variable para método get
                const diaNuevo = daySaleArray.some(a => a.includes(ayer)) //revisar si el arreglo contiene el 27, siendo que hoy es 28
                if(diaNuevo){
                    console.log("------")
                    console.log("Nuevo dia "+new Date().toLocaleDateString("es-CL")+" - Imprimiendo ventas del día "+ayer)
                    console.log("------")
                     //si contiene el 27, significa que paso el día, por lo que hay que imprimir el arreglo.
                    console.log(daySaleArray); // Imprimir todas las ventas generadas en el día
                    daySaleArray = [];
                    ventasDailyArray = [];
                }
            }
        },
    })
}
app.get("/ventas", async (req, res) => {
    res.status(200).json({"Ventas del día": ventasDailyArray}); // Obtener ventas generadas en el día HASTA AHORA.
});
 
app.listen(port, () => {
    console.log(`Escuchando en puerto: ${port}`);
    ventasDiarias()
});