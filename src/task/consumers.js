const consumers = {
  visits: (timestamp, data) => {
    const { userId, ua, ip, referer, app, pageUrl } = data;
    return { userId, ua, ip, referer, app, timestamp, pageUrl };
  },
  events: (timestamp, data) => {
    const { eventId, userId, questionId, answerId, app } = data;
    return { userId, app, eventId, questionId, answerId, timestamp };
  },
  recommendations: (timestamp, data) => {
    const { userId, fromUrl, toUrl, app } = data;
    return { userId, fromUrl, toUrl, app, timestamp };
  }
};

module.exports = consumers;
