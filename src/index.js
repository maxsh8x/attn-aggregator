const amqp = require("amqplib");
const express = require("express");
const geoip = require("geoip-lite");
const mongoose = require("mongoose");
const parser = require("ua-parser-js");
const bodyParser = require("body-parser");
const ClickHouse = require("@apla/clickhouse");
const { URL } = require('url');

const config = require("./utils/config");
const models = require("./models");

const app = express();
app.use(bodyParser.json());

mongoose.connect(config.mongoUri, { useMongoClient: true });
mongoose.Promise = global.Promise;

const toISODate = unixTime =>
  new Date(unixTime * 1000).toISOString().slice(0, 10);

async function run() {
  const amqpConn = await amqp.connect(config.amqp.host);
  const amqpCh = await amqpConn.createChannel();

  await amqpCh.assertQueue("visits", { durable: true });
  await amqpCh.assertQueue("events", { durable: true });
  await amqpCh.assertQueue("recommendations", { durable: true });

  const clickhouseConn = new ClickHouse(config.clickhouse);

  const visitsBuff = [];
  amqpCh.consume("visits", msg => {
    const data = JSON.parse(msg.content);
    const timestamp = msg.properties.timestamp;
    const { userId, ua, ip, referer, app, pageUrl } = data;
    visitsBuff.push([msg, { userId, ua, ip, referer, app, timestamp, pageUrl }]);
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
    recommsBuff.push([
      msg,
      { userId, fromUrl, toUrl, app, timestamp }
    ]);
  });

  app.post("/api/v1/ua/", async function({ body: { ua } }, res) {
    const { browser, device, os } = parser(ua);
    const promises = [];
    if (browser.name) {
      promises.push(new models.Browser({ name: browser.name.toLowerCase() }).save());
    }
    if (device.type) {
      promises.push(new models.DeviceType({ name: device.type.toLowerCase() }).save());
    }
    if (device.vendor) {
      promises.push(new models.DeviceVendor({ name: device.vendor.toLowerCase() }).save());
    }
    if (os.name) {
      promises.push(new models.OperationSystem({ name: os.name.toLowerCase() }).save());
    }
    await Promise.all(promises).catch(err => console.error(err));
    res.send(JSON.stringify("ok", null, "  "));
  });

  app.listen(config.port, () => console.info(`App listening on port ${config.port}!`));

  const timeout = ms => new Promise(resolve => setTimeout(resolve, ms));

  while (true) {
    const dicts = {
      event: new Map(),
      browser: new Map(),
      UTM_Medium: new Map(),
      UTM_Source: new Map(),
      deviceType: new Map(),
      deviceVendor: new Map(),
      operationSystem: new Map(),
    };

    const eventDictData = await models.Event.find({}, "-_id code name");
    for (let i = 0; i < eventDictData.length; i++) {
      const { code, name } = eventDictData[i];
      dicts.event.set(name, code);
    }

    const browserDictData = await models.Browser.find({}, "-_id code name");
    for (let i = 0; i < browserDictData.length; i++) {
      const { code, name } = browserDictData[i];
      dicts.browser.set(name, code);
    }

    const deviceTypeData = await models.DeviceType.find({}, "-_id code name");
    for (let i = 0; i < deviceTypeData.length; i++) {
      const { code, name } = deviceTypeData[i];
      dicts.deviceType.set(name, code);
    }

    const deviceVendorData = await models.DeviceVendor.find(
      {},
      "-_id code name"
    );
    for (let i = 0; i < deviceVendorData.length; i++) {
      const { code, name } = deviceVendorData[i];
      dicts.deviceVendor.set(name, code);
    }

    const operationSystemData = await models.OperationSystem.find(
      {},
      "-_id code name"
    );
    for (let i = 0; i < operationSystemData.length; i++) {
      const { code, name } = operationSystemData[i];
      dicts.operationSystem.set(name, code);
    }

    const UTM_MediumData = await models.OperationSystem.find(
      {},
      "-_id code name"
    );
    for (let i = 0; i < UTM_MediumData.length; i++) {
      const { code, name } = UTM_MediumData[i];
      dicts.UTM_Medium.set(name, code);
    }

    const UTM_SourceData = await models.OperationSystem.find(
      {},
      "-_id code name"
    );
    for (let i = 0; i < UTM_SourceData.length; i++) {
      const { code, name } = UTM_SourceData[i];
      dicts.UTM_Source.set(name, code);
    }
    
    const convertToInt = (dict, value) => {
      if (typeof value === "undefined" || value === null) {
        return 0;
      }
      const convertedValue = dicts[dict].get(value.toLowerCase());
      return typeof convertedValue === "undefined" ? 0 : convertedValue;
    };

    /* ----------------------------------------- */

    const visitsRawData = visitsBuff.splice(0);
    if (visitsRawData.length > 0) {
      const clickhouseStream = clickhouseConn.query(
        "INSERT INTO aggregator_visits",
        { format: "JSONEachRow" },
        err => {
          if (!err) {
            visitsRawData.forEach(([msg]) => amqpCh.ack(msg))
          }
        }
      );

      for (let i = 0; i < visitsRawData.length; i++) {
        const { browser, device, os } = parser(visitsRawData[i][1].ua);
        const { ll } = geoip.lookup(visitsRawData[i][1].ip);
        const date = new Date(visitsRawData[i][1].timestamp * 1000);
        const sourceURL = new URL(visitsRawData[i][1].pageUrl);

        clickhouseStream.write({
          userId: visitsRawData[i][1].userId,
          app: visitsRawData[i][1].appId,
          ip: visitsRawData[i][1].ip,
          ua: visitsRawData[i][1].ua,
          referer: visitsRawData[i][1].referer,
          pagePath: sourceURL.pathname,
          UTM_Source: convertToInt('UTM_Source', sourceURL.searchParams.get('utm_source')), 
          UTM_Medium: convertToInt('UTM_Medium', sourceURL.searchParams.get('utm_medium')), 
          UTM_Campaign: sourceURL.searchParams.get('utm_campaign') || '', 
          UTM_Content: sourceURL.searchParams.get('utm_content') || '', 
          UTM_Term: sourceURL.searchParams.get('utm_term') || '', 
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
            eventsRawData.forEach(([msg]) => amqpCh.ack(msg))
          }
        }
      );

      for (let i = 0; i < eventsRawData.length; i++) {
        const date = new Date(eventsRawData[i][1].timestamp * 1000);
        clickhouseStream.write({
          userId: eventsRawData[i][1].userId,
          appId: eventsRawData[i][1].app,
          eventId: convertToInt("event", eventsRawData[i][1].event),
          questionId: eventsRawData[i][1].questionId,
          answerId: eventsRawData[i][1].answerId,
          eventTime: date.toLocaleString(),
          eventDate: date.toLocaleDateString(),
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
            recommsRawData.forEach(([msg]) => amqpCh.ack(msg))
          }
        }
      );

      for (let i = 0; i < recommsRawData.length; i++) {
        const date = new Date(recommsRawData[i][1].timestamp * 1000);
        clickhouseStream.write({
          userId: recommsRawData[i][1].userId,
          appId: recommsRawData[i][1].app,
          fromUrl: recommsRawData[i][1].fromUrl,
          toUrl: recommsRawData[i][1].toUrl,
          eventTime: date.toLocaleString(),
          eventDate: date.toLocaleDateString(),
        });
      }

      clickhouseStream.end();

    }      

    await timeout(1000);
  }
}

run().catch(error => console.error(error.stack));
