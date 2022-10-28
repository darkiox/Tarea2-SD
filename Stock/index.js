const express = require("express");
const app = express();

const { Client } = require('pg')

const { Kafka } = require('kafkajs')
app.use(express.json());
const client = new Client({
    database: 'tarea',
    host: 'db-tarea',
    user: 'postgres',
    password: 'postgres',
    port: 5432,
})
client.connect(function(err){
    if (err) console.log("Error al conectar a DB");
    console.log("Conectado a DB.")
})
const port = process.env.PORT;

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});

var RenovarStock = [];
var ncarritos = 0;
const stock = async () => {
    const consumer = kafka.consumer({ groupId: 'stock', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'stock' });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            if (message.value){
                var data = JSON.parse(message.value.toString());
                var count = "SELECT count(*) FROM carritos WHERE stock < 20"
                await client.query(count, async (err, res)=> {
                    if (err){
                        console.log("Error en count.")
                    } else{
                        console.log("n carritos:" + res)
                        carritos = res
                    }
                })
                for (var i = 0; i < carritos; i+=5) {
                    var query = "SELECT patente , stock from carritos WHERE stock < 20 LIMIT 5 OFFSET "+i+";"
                    await client.query(query, async (err, res)=> {
                        if (err){
                            console.log("Error en query.")
                        } else{
                            for (var j = 0; j < 5; j++) {
                                try{
                                    RenovarStock.includes(res.rows[j].patente)
                                }
                                catch{
                                    break;
                                }
                            }
                        }
                    })
                }
            }
        },
    })
}
app.get("/Stock", async (req, res) => {
    res.status(200).json({"Carritos a renovar stock": RenovarStock});
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
    stock();
});