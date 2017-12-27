const modelByDict = modelName =>
  modelName.charAt(0).toUpperCase() + modelName.slice(1);

module.exports = { modelByDict };
