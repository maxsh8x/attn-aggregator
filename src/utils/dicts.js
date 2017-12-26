const models = require("../models");
const { modelByDict } = require("./name");

const initDicts = dictNames => {
  const dicts = {};
  dictNames.forEach(name => {
    dicts[name] = new Map();
  });
  return dicts;
};

async function fillDicts(dicts) {
  for (let dictName in dicts) {
    const data = await models[modelByDict(dictName)].find(
      {},
      "-_id code name"
    );
    for (let { code, name } of data) {
      dicts[dictName].set(name, code);
    }
  } 
};

module.exports = { initDicts, fillDicts };
