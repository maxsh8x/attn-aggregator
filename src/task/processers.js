const parser = require("ua-parser-js");
const geoip = require("geoip-lite");
const { URL } = require("url");

const { convertToInt } = require("../lib/converter");

const processers = {
  visits: (item, dicts) => {
    const { browser, device, os } = parser(item.ua);
    const { ll } = geoip.lookup(item.ip);
    const sourceURL = new URL(item.pageUrl);
    return {
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
      longitude: ll[0],
      latitude: ll[1]
    };
  },
  events: (item, dicts) => {
    return {
      userId: item.userId,
      appId: item.app,
      eventId: convertToInt(dicts.event, item.event),
      questionId: item.questionId,
      answerId: item.answerId
    };
  },
  recommendations: (item, dicts) => {
    return {
      userId: item.userId,
      appId: item.app,
      fromUrl: item.fromUrl,
      toUrl: item.toUrl
    };
  }
};

module.exports = processers;
