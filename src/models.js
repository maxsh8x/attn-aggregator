const mongoose = require("mongoose");
const AutoIncrement = require("mongoose-sequence")(mongoose);

const config = require("./lib/config");
const { modelByDict } = require("./lib/name");

function GetModels() {
  const models = {};
  for (let dictName of config.dictionaries) {
    const Data = mongoose.Schema({
      name: String,
      code: Number,
      description: String,
    });
    Data.plugin(AutoIncrement, {
      inc_field: "code",
      id: `${dictName.toLowerCase()}_seq`
    });
    Data.index({ code: 1 }, { unique: true });
    Data.index({ name: 1 }, { unique: true });
    const modelName = modelByDict(dictName);
    models[modelName] = mongoose.model(modelName, Data, modelName);
  }
  return models;
}

module.exports = new GetModels();
