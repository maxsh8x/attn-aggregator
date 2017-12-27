const express = require("express");
const mongoose = require("mongoose");
const parser = require("ua-parser-js");
const bodyParser = require("body-parser");

const config = require("./lib/config");
const { runTasks } = require("./tasker");

const app = express();
app.use(bodyParser.json());

mongoose.connect(config.mongoUri, { useMongoClient: true });
mongoose.Promise = global.Promise;

async function run() {
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
    console.info(`API listening on port ${config.port}!`)
  );

  await runTasks();
}

run().catch(error => console.error(error.stack));
