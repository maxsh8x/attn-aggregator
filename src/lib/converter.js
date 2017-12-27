const convertToInt = (dicts, dict, value) => {
  if (typeof value === "undefined" || value === null) {
    return 0;
  }
  const convertedValue = dicts[dict].get(value.toLowerCase());
  return typeof convertedValue === "undefined" ? 0 : convertedValue;
};

module.exports = { convertToInt };
