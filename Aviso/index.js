
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

app.use(express.json());

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});
const producer = kafka.producer();
const ingresarUsuario = async (nombre, apellido, rut, correodueno, patente, premium, stock, ubicacion) => {
    // Agradecimientos a "Richard-Bevan"
    // https://dirask.com/posts/Node-js-PostgreSQL-Insert-query-DZXq2j
    try{
        var query1 =  `CREATE TABLE IF NOT EXISTS carritos(
            nombre VARCHAR(50), 
            apellido VARCHAR(50), 
            rut VARCHAR(50), 
            correodueno VARCHAR(200), 
            patente VARCHAR(10), 
            premium INT, 
            stock INT, 
            ubicacion VARCHAR(10)
            );`
        var query2 = `INSERT INTO carritos VALUES ('`+nombre+`','`+apellido+`','`+rut+`','`+correodueno+`','`+patente+`','`+premium+`','`+stock+`','`+ubicacion+`');`
        await client.query(query1);
        await client.query(query2);
        return true;
    } catch (error) {
        console.log("Ha ocurrido un error en el ingreso a la base de datos.")
        return false;
    }
}

const usuarioRepetido = async (rut) =>{
    try {
        var query1 =  `CREATE TABLE IF NOT EXISTS carritos(
            nombre VARCHAR(50), 
            apellido VARCHAR(50), 
            rut VARCHAR(50), 
            correodueno VARCHAR(200), 
            patente VARCHAR(10), 
            premium INT, 
            stock INT, 
            ubicacion VARCHAR(10)
            );`
        await client.query(query1)
        var querySelect = `SELECT rut FROM carritos WHERE "rut" = '`+rut+`';`
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

    console.log(new Date(req.body.time).toLocaleDateString("es-CL"), '|' , new Date(req.body.time).toLocaleTimeString("es-CL"), '|','"'+req.body.nombre+'"'+"está registrandose.");
    await producer.connect();
    await usuarioRepetido(req.body.rut).then(async result =>{
        if(result){
            producer.disconnect().then(
                res.status(403).json("El mastro sopaipillero ya existe."),
                )
                console.log("El mastro sopaipillero '"+req.body.nombre+"' ya existe.")
        }else{
            await producer.send({
                topic: 'register',
                messages: [{value: JSON.stringify(req.body)}]
            })
            await ingresarUsuario(  req.body.nombre, 
                                    req.body.apellido,
                                    req.body.rut,
                                    req.body.correodueno,
                                    req.body.patente,
                                    req.body.premium,
                                    req.body.stock,
                                    req.body.ubicacion).then(result => {
                if(result){
                    producer.disconnect().then(
                        res.status(200).json("Mastro sopaipillero registrado.")
                    )
                    console.log("Maestro sopaipillero "+req.body.nombre+" registrado correctamente.")
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