const express = require("express");
const mongoose = require("mongoose");
const parser = require("ua-parser-js");
const bodyParser = require("body-parser");

const config = require("./lib/config");
const { runTasks } = require("./tasker");
const models = require("./models");
const { modelByDict } = require("./lib/name");

const app = express();
app.use(bodyParser.json());

mongoose.connect(config.mongoUri, { useMongoClient: true });
mongoose.Promise = global.Promise;

async function run() {
  for (let dictName of config.dictionaries) {
    const modelName = modelByDict(dictName);
    app.post(`/api/v1/${modelName}`, async function({ body: { name } }, res) {
      await new models[modelName]({ name }).save();
    });
  }

  app.listen(config.port, () =>
    console.info(`API listening on port ${config.port}!`)
  );

  await runTasks();
}

run().catch(error => console.error(error.stack));
