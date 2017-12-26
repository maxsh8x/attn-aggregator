const amqp = require("amqplib");
const express = require("express");
const geoip = require("geoip-lite");
const mongoose = require("mongoose");
const parser = require("ua-parser-js");
const bodyParser = require("body-parser");
const ClickHouse = require("@apla/clickhouse");
const { URL } = require("url");

const config = require("./utils/config");
const { convertToInt } = require("./utils/converter");
const { initDicts, fillDicts } = require("./utils/dicts");
const models = require("./models");

const app = express();
app.use(bodyParser.json());

mongoose.connect(config.mongoUri, { useMongoClient: true });
mongoose.Promise = global.Promise;

async function run() {
  const amqpConn = await amqp.connect(config.amqp.host);
  const amqpCh = await amqpConn.createChannel();

  for (let queue of ["visits", "events", "recommendations"]) {
    await amqpCh.assertQueue(queue, { durable: true });
  }

  const clickhouseConn = new ClickHouse(config.clickhouse);

  const visitsBuff = [];
  amqpCh.consume("visits", msg => {
    const data = JSON.parse(msg.content);
    const timestamp = msg.properties.timestamp;
    const { userId, ua, ip, referer, app, pageUrl } = data;
    visitsBuff.push([
      msg,
      { userId, ua, ip, referer, app, timestamp, pageUrl }
    ]);
  });

  const eventsBuff = [];
  amqpCh.consume("events", msg => {
    const data = JSON.parse(msg.content);
    const timestamp = msg.properties.timestamp;
    const { eventId, userId, questionId, answerId, app } = data;
    eventsBuff.push([
      msg,
      { userId, app, eventId, questionId, answerId, timestamp }
    ]);
  });

  const recommsBuff = [];
  amqpCh.consume("recommendations", msg => {
    const data = JSON.parse(msg.content);
    const timestamp = msg.properties.timestamp;
    const { userId, fromUrl, toUrl, app } = data;
    recommsBuff.push([msg, { userId, fromUrl, toUrl, app, timestamp }]);
  });

  app.post("/api/v1/ua/", async function({ body: { ua } }, res) {
    const { browser, device, os } = parser(ua);
    const promises = [];
    if (browser.name) {
      promises.push(
        new models.Browser({ name: browser.name.toLowerCase() }).save()
      );
    }
    if (device.type) {
      promises.push(
        new models.DeviceType({ name: device.type.toLowerCase() }).save()
      );
    }
    if (device.vendor) {
      promises.push(
        new models.DeviceVendor({ name: device.vendor.toLowerCase() }).save()
      );
    }
    if (os.name) {
      promises.push(
        new models.OperationSystem({ name: os.name.toLowerCase() }).save()
      );
    }
    await Promise.all(promises).catch(err => console.error(err));
    res.send(JSON.stringify("ok", null, "  "));
  });

  app.listen(config.port, () =>
    console.info(`App listening on port ${config.port}!`)
  );

  const timeout = ms => new Promise(resolve => setTimeout(resolve, ms));

  while (true) {
    const dicts = initDicts(config.dictionaries);
    await fillDicts(dicts);

    /* ----------------------------------------- */

    const visitsRawData = visitsBuff.splice(0);
    if (visitsRawData.length > 0) {
      const clickhouseStream = clickhouseConn.query(
        "INSERT INTO aggregator_visits",
        { format: "JSONEachRow" },
        err => {
          if (!err) {
            visitsRawData.forEach(([msg]) => amqpCh.ack(msg));
          } else {
            console.error(err)
          }
        }
      );

      for (let [, item] of visitsRawData) {
        const { browser, device, os } = parser(item.ua);
        const { ll } = geoip.lookup(item.ip);
        const date = new Date(item.timestamp * 1000);
        const sourceURL = new URL(item.pageUrl);

        clickhouseStream.write({
          userId: item.userId,
          appId: item.app,
          ip: item.ip,
          ua: item.ua,
          referer: item.referer,
          pagePath: sourceURL.pathname,
          UTMSource: convertToInt(
            "UTMSource",
            sourceURL.searchParams.get("utm_source")
          ),
          UTMMedium: convertToInt(
            "UTMMedium",
            sourceURL.searchParams.get("utm_medium")
          ),
          UTMCampaign: sourceURL.searchParams.get("utm_campaign") || "",
          UTMContent: sourceURL.searchParams.get("utm_content") || "",
          UTMTerm: sourceURL.searchParams.get("utm_term") || "",
          browserName: convertToInt("browser", browser.name),
          browserMajorVersion: browser.major || 0,
          deviceType: convertToInt("deviceType", device.type),
          deviceVendor: convertToInt("deviceVendor", device.vendor),
          operationSystem: convertToInt("operationSystem", os.name),
          eventTime: date.toLocaleString(),
          eventDate: date.toLocaleDateString(),
          longitude: ll[0],
          latitude: ll[1]
        });
      }

      clickhouseStream.end();
    }

    /* ----------------------------------------- */

    const eventsRawData = eventsBuff.splice(0);
    if (eventsRawData.length > 0) {
      const clickhouseStream = clickhouseConn.query(
        "INSERT INTO aggregator_events",
        { format: "JSONEachRow" },
        err => {
          if (!err) {
            eventsRawData.forEach(([msg]) => amqpCh.ack(msg));
          }
        }
      );

      for (let [, item] of eventsRawData) {
        const date = new Date(item.timestamp * 1000);
        clickhouseStream.write({
          userId: item.userId,
          appId: item.app,
          eventId: convertToInt("event", item.event),
          questionId: item.questionId,
          answerId: item.answerId,
          eventTime: date.toLocaleString(),
          eventDate: date.toLocaleDateString()
        });
      }

      clickhouseStream.end();
    }

    /* ----------------------------------------- */

    const recommsRawData = recommsBuff.splice(0);
    if (recommsRawData.length > 0) {
      const clickhouseStream = clickhouseConn.query(
        "INSERT INTO aggregator_recommendations",
        { format: "JSONEachRow" },
        err => {
          if (!err) {
            recommsRawData.forEach(([msg]) => amqpCh.ack(msg));
          }
        }
      );

      for (let [, item] of recommsRawData) {
        const date = new Date(item.timestamp * 1000);
        clickhouseStream.write({
          userId: item.userId,
          appId: item.app,
          fromUrl: item.fromUrl,
          toUrl: item.toUrl,
          eventTime: date.toLocaleString(),
          eventDate: date.toLocaleDateString()
        });
      }

      clickhouseStream.end();
    }

    await timeout(config.taskInterval);
  }
}

run().catch(error => console.error(error.stack));
