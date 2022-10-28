const express = require("express");
const app = express();

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
app.use(express.json());
const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});

const producer = kafka.producer();
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Math/random
function getRandomInt(max) {
    return Math.floor(Math.random() * max);
  }
// Revisar si patente existe
const checkPatente = async (patente) => {
    try{
        var queryCheckPatente = `SELECT count(*) FROM carritos WHERE patente = '`+patente+`';`
        const res = await client.query(queryCheckPatente)
        var existe = res.rows[0].count
        if (existe == 1){
            return true;
        }else{
            return false;
        }
    } catch (err) {
        return false;
    }
}
const denunciarCarrito = async (patente, ubicacion) => {
    try{
        var queryUpdate = `UPDATE carritos SET ubicacion = '`+ubicacion+`', profugo =`+1+` WHERE patente ='`+patente+`';`
        await client.query(queryUpdate);
        await producer.send({
            topic: 'ubicacion',
            messages: [{value: JSON.stringify({"patente": patente, "ubicacion": ubicacion})}],
            partition: 1
        }).then(
            console.log(ubicacion + " => topic: 'ubicacion' | partition: '1'")
        )
        return true;
    } catch (error) {
        console.log("Ha ocurrido un error en el ingreso a la base de datos.")
        return false;
    }
}
app.post("/aviso", async (req, res) => {
    req.body.time = new Date().getTime();
    console.log(new Date(req.body.time).toLocaleDateString("es-CL"), '|' , new Date(req.body.time).toLocaleTimeString("es-CL"), '|','El carrito de patente "'+req.body.patente+'"'+" es denunciado profugo.");
    await producer.connect();

    await checkPatente(req.body.patente).then(async result => {
     if(result){
         await denunciarCarrito(req.body.patente ,req.body.ubicacion).then(async result => {
             if (result){
                 producer.disconnect().then(
                     res.status(200).json("Profugo registrado."),
                 )
                 console.log("Profugo registrado.")
             }else{
                 producer.disconnect().then(
                     res.status(500).json("Error.")
                 )
                 console.log("Error.")
             }   

     })
    }else{
        producer.disconnect().then(
            res.status(401).json("La patente '"+req.body.patente+"' no existe.")
        )
        console.log("Se ingresó una patente que no existe. ('"+req.body.patente+"')")
    }

        console.log("-------")
    })
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
});