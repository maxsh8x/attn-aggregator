const amqp = require("amqplib");

const config = require("./utils/config");
const uaArr = require(`../ua.json`);

async function run() {
  const amqpConn = await amqp.connect(config.amqp.host);
  const amqpCh = await amqpConn.createChannel();

  uaArr.forEach(item => {
    const msg = {
      userId: 1,
      app: "theanswer",
      ua: item,
      ip: "176.59.77.204",
      referer: "https://t.me"
    };
    amqpCh.sendToQueue("visits", Buffer.from(JSON.stringify(msg)), {
      timestamp: 1513865966
    });
  });
}

run().catch(error => console.error(error.stack));
