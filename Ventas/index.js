const express = require("express");
const { Kafka } = require('kafkajs')
const { Client } = require('pg')
const client = new Client({
  user: 'postgres',
  host: 'database',
  database: 'tarea',
  password: 'postgres',
  port: 5432,
})
client.connect(function(err){
    if (err) console.log("Error al conectar a DB");
    console.log("Conectado a DB.")
})
const port = process.env.PORT;
const app = express();

app.use(express.json());

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});

const producer = kafka.producer();
const registrarVenta = async (patente, cliente, cantidad) => {
    // Agradecimientos a "Richard-Bevan"
    // https://dirask.com/posts/Node-js-PostgreSQL-Insert-query-DZXq2j
    try{
        var query1 =  `CREATE TABLE IF NOT EXISTS ventas (
            "patente" VARCHAR(50),
            "cliente" VARCHAR(100),
            "#Sopaipillas" int
            );`
        var query2 = `INSERT INTO ventas VALUES ('`+patente+`','`+cliente+`','`+cantidad+`');`
        var query3 = `UPDATE carritos SET stock = stock -`+cantidad+`, ubicacion = '`+Math.random().toString()+`' WHERE patente ='`+patente+`';`
        await client.query(query1);
        await client.query(query2);
        await client.query(query3);
        return true;
    } catch (error) {
        console.log("Ha ocurrido un error en el ingreso a la base de datos.")
        return false;
    }
}
app.post("/ventas", async (req, res) => {
    req.body.time = new Date().getTime();
    console.log(new Date(req.body.time).toLocaleDateString("es-CL"), '|' , new Date(req.body.time).toLocaleTimeString("es-CL"), '|','"'+req.body.cliente+'"'+" está registrando una venta.");
    await producer.connect();

    await registrarVenta(req.body.patente ,req.body.cliente, req.body.cantidad).then(async result => {
        if (result){
            await producer.send({
                topic: 'ventas',
                messages: [{value: JSON.stringify(req.body)}]
            })
            producer.disconnect().then(
                res.status(200).json("Venta registrada"),
            )
            console.log("Venta registrada.")
        }else{
            producer.disconnect().then(
                res.status(500).json("Error.")
            )
            console.log("Error.")
        }
    
        console.log("-------")
    })
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
});