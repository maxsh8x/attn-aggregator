const initDicts = dictNames => {
  const dicts = {}
  dictNames.forEach(name => {
    dicts[name] = new Map()
  })
  return dicts
};

module.exports = { initDicts };
