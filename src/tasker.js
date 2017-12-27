const amqp = require("amqplib");
const geoip = require("geoip-lite");
const parser = require("ua-parser-js");
const ClickHouse = require("@apla/clickhouse");
const { URL } = require("url");

const config = require("./lib/config");
const { convertToInt } = require("./lib/converter");
const { initDicts, fillDicts } = require("./lib/dicts");
const models = require("./models");

async function runTasks() {
  const amqpConn = await amqp.connect(config.amqp.host);
  const amqpCh = await amqpConn.createChannel();
  const clickhouseConn = new ClickHouse(config.clickhouse);

  for (let queue of ["visits", "events", "recommendations"]) {
    await amqpCh.assertQueue(queue, { durable: true });
  }

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
            console.error(err);
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
            dicts.UTMSource,
            sourceURL.searchParams.get("utm_source")
          ),
          UTMMedium: convertToInt(
            dicts.UTMMedium,
            sourceURL.searchParams.get("utm_medium")
          ),
          UTMCampaign: sourceURL.searchParams.get("utm_campaign") || "",
          UTMContent: sourceURL.searchParams.get("utm_content") || "",
          UTMTerm: sourceURL.searchParams.get("utm_term") || "",
          browserName: convertToInt(dicts.browser, browser.name),
          browserMajorVersion: browser.major || 0,
          deviceType: convertToInt(dicts.deviceType, device.type),
          deviceVendor: convertToInt(dicts.deviceVendor, device.vendor),
          operationSystem: convertToInt(dicts.operationSystem, os.name),
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
          eventId: convertToInt(dicts.event, item.event),
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

module.exports = { runTasks };
