const express = require('express')
const avsc = require('avro-js')
const cors = require('cors')
const bodyParser = require('body-parser');
const {createObjKeys,fillObj}  = require('./utils/parser')
const app = express()
const port = 3001
const { Kafka, Partitioners } = require('kafkajs')

const fs = require("fs");
const readline = require("readline");

const stream = fs.createReadStream("./myFile01.csv");
const rl = readline.createInterface({ input: stream });
let data = [];
// console.log("data", data)

rl.on("line", (row) => {
  data.push(row.split(","));
});
let result = {}
rl.on("close", () => {
   result = fillObj(data)
  console.log(result)
});


app.use(bodyParser.urlencoded({
  extended: false
}))
app.use(bodyParser.json())

const kafka_ = new Kafka({
  clientId: 'qa-topic',
  brokers: ['pkc-ldvmy.centralus.azure.confluent.cloud:9092'],
  ssl: true,
  logLevel: 2,
  sasl: {
    mechanism: 'PLAIN',
    username: '4DK6J6FQZC4DFV2M',
    password: 'ikJC5neEKJMAAdayGatMWlWWUV/S3Ua1gFq6XBVUHwxspJMdiqs5Ljna/WxOrGim'
  }
})

const consumer = kafka_.consumer({ groupId: 'test-group' })
const producer = kafka_.producer({
  createPartitioner: Partitioners.LegacyPartitioner
})

const type = avsc.parse({
  name: 'kafka',
  type: 'record',
  fields: [
    { name: 'PROFILE_ID', type: 'string' },
    { name: 'PROGRAM_CODE', type: 'string' },
    { name: 'YEAR', type: 'string' },
    { name: 'PROMOTION_NAME', type: 'string' },
    { name: 'PROGRESS_INDICATOR', type: 'string' },
    { name: 'PROGRESS_VALUE', type: 'string' },
    { name: 'UPDATE_DATE', type: 'string' },
  ]
})

app.get('/producer', async (req, res) => {
  // Producing
  try {
    console.log("result",result)
    await producer.connect()
    await producer.send({
      topic: 'TP.MTL.PROFILE.TIER_STATUS_UPDATE_US',
      messages: [{ value: type.toBuffer(result) },
      ],
    })
    res.send('producer')
  } catch (error) {
    console.log(error)
  }
})


app.get('/consumer', async (req, res) => {
  try {
    await consumer.connect()
    await consumer.subscribe({ topic: 'TP.MTL.PROFILE.TIER_STATUS_UPDATE_US', fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          partition,
          offset: message.offset,
          value: type.fromBuffer(message.value),
        })
      },
    })
    res.send('consumer')
  } catch (error) {

  }
})

app.use(cors())
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Authorization, X-API-KEY, Origin, X-Requested-With, Content-Type, Accept, Access-Control-Allow-Request-Method"
  );
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE");
  res.header("Allow", "GET, POST, OPTIONS, PUT, DELETE");
  next();
});


app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})



// const kafka = async () => {

//   const kafka = new Kafka({
//     clientId: 'qa-topic',
//     brokers: ['xxxxxxxxx.confluent.cloud:9092'],
//     ssl: true,
//     logLevel: 2,
//     sasl: {
//       mechanism: 'plain',
//       username: 'xxxxxxxxxxx',
//       password: 'xxxxxxxxxx'
//     }
//   })

// }


  // const stream = fs.createReadStream("./myFile0.csv");
  // const rl = readline.createInterface({ input: stream });
  // let data = [];
  // console.log("data", data)

  // rl.on("line", (row) => {
  //     data.push(row.split(","));
  // });

  // rl.on("close", () => {
  //     console.log(data);
  // });