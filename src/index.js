const express = require("express");
const mongoose = require("mongoose");
const parser = require("ua-parser-js");
const bodyParser = require("body-parser");

const config = require("./lib/config");
const { runTasks } = require("./task");
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
      if (typeof name !== 'string') {
        return res.status(400).send('name must be string')
      }
      await new models[modelName]({ name }).save();
      res.send('ok')
    });
  }

  app.listen(config.port, () =>
    console.info(`API listening on port ${config.port}!`)
  );

  await runTasks();
}

run().catch(error => console.error(error.stack));
