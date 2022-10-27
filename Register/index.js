const express = require("express");
const { Kafka } = require('kafkajs')
const { Client } = require('pg')
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
const app = express();

app.use(express.json());

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});
var registerResult;
const producer = kafka.producer();
const ingresarUsuario = async (user, password) => {
    // Agradecimientos a "Richard-Bevan"
    // https://dirask.com/posts/Node-js-PostgreSQL-Insert-query-DZXq2j
    try{
        var query1 =  `CREATE TABLE IF NOT EXISTS users (
            "user" VARCHAR(50),
            password VARCHAR(100)
            );`
        var query2 = `INSERT INTO users VALUES ('`+user+`','`+password+`');`
        await client.query(query1);
        await client.query(query2);
        return true;
    } catch (error) {
        console.log("Ha ocurrido un error en el ingreso a la base de datos.")
        return false;
    }
}

const usuarioRepetido = async (user) =>{
    try {
        var query1 =  `CREATE TABLE IF NOT EXISTS users (
            "user" VARCHAR(50),
            password VARCHAR(100)
            );`
        await client.query(query1)
        var querySelect = `SELECT "user" FROM users WHERE "user" = '`+user+`';`
        const res = await client.query(querySelect)
        if (res.rows[0]){
            // Usuario repetido
            return true
        }else{
            // Usuario no existe en BD
            return false
        }
    } catch (err) {
        return false;
    }
}
app.post("/register", async (req, res) => {
    req.body.time = new Date().getTime();

    console.log(new Date(req.body.time).toLocaleDateString("es-CL"), '|' , new Date(req.body.time).toLocaleTimeString("es-CL"), '|','"'+req.body.user+'"'+"está registrandose.");
    await producer.connect();
    await usuarioRepetido(req.body.user).then(async result =>{
        if(result){
            producer.disconnect().then(
                res.status(403).json("Usuario ya existe."),
                )
                console.log("El usuario '"+req.body.user+"' ya existe.")
        }else{
            await producer.send({
                topic: 'register',
                messages: [{value: JSON.stringify(req.body)}]
            })
            await ingresarUsuario(req.body.user, req.body.password).then(result => {
                if(result){
                    producer.disconnect().then(
                        res.status(200).json("Usuario registrado.")
                    )
                    console.log("Usuario "+req.body.user+" registrado correctamente.")
                }else{
                    producer.disconnect().then(
                        res.status(500).json("Error 500.")
                    )
                }
            })
        }
        console.log('-------')
    })
});
app.get("/test", async (req,res) =>{
    res.status(200).json({
        hola: 'grupo'
    })
})
app.listen(port, () => {
    console.log(`Listening on port ${port}`);
});