const Joi = require("joi");

const validators = {
  visits: Joi.object()
    .keys({
      userId: Joi.number()
        .min(0)
        .max(4294967295), // UInt32
      app: Joi.string().valid("theanswer", "thesalt"),
      ua: Joi.string(),
      ip: Joi.string().ip({ version: ["ipv4"] }),
      referer: Joi.string().uri(),
      pageUrl: Joi.string().uri()
    })
    .with("userId", "app", "ua", "ip", "referer", "pageUrl"),
  events: Joi.object().with("userID", "app", "event", "questionId", "answerId"),
  recommendations: Joi.object().with("userId", "app", "fromUrl", "toUrl")
};

module.exports = validators;
