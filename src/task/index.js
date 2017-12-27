const amqp = require("amqplib");
const ClickHouse = require("@apla/clickhouse");

const config = require("../lib/config");
const { initDicts, fillDicts } = require("../lib/dicts");
const models = require("../models");
const consumers = require("./consumers");
const processers = require("./processers");

const timeout = ms => new Promise(resolve => setTimeout(resolve, ms));

async function runTasks() {
  const amqpConn = await amqp.connect(config.amqp.host);
  const amqpCh = await amqpConn.createChannel();
  const clickhouseConn = new ClickHouse(config.clickhouse);

  const buffers = {};

  for (let queue in consumers) {
    await amqpCh.assertQueue(queue, { durable: true });
  }

  Object.keys(consumers).forEach(name => {
    buffers[name] = [];
    amqpCh.consume(name, msg => {
      const data = JSON.parse(msg.content);
      const timestamp = new Date(msg.properties.timestamp * 1000);
      const result = consumers[name](timestamp, data);
      buffers[name].push([
        msg,
        {
          ...result,
          eventTime: timestamp.toLocaleString(),
          eventDate: timestamp.toLocaleDateString()
        }
      ]);
    });
  });

  while (true) {
    const dicts = initDicts(config.dictionaries);
    await fillDicts(dicts);

    for (let name in buffers) {
      const data = buffers[name].splice(0);
      if (data.length > 0) {
        const clickhouseStream = clickhouseConn.query(
          `INSERT INTO aggregator_${name}`,
          { format: "JSONEachRow" },
          err => {
            if (!err) {
              for (let [msg] of data) {
                amqpCh.ack(msg);
              }
            } else {
              console.error(err);
            }
          }
        );

        for (let [, item] of data) {
          clickhouseStream.write(processers[name](item, dicts));
        }

        clickhouseStream.end();
      }
    }

    await timeout(config.taskInterval);
  }
}

module.exports = { runTasks };
