const { Client } = require('pg');

exports.handler = async function (event, context, callback) {
  const message = event.Records[0].messageAttributes;
  const client = new Client();
  await client.connect();

  if (message.public.stringValue === 'true') {
    await client.query({
      text: "INSERT INTO events (value, metric, user_name, client_id) VALUES ($1,$2,$3,$4)",
      values: [parseFloat(message.value.stringValue), message.metric.stringValue, message.user_name.stringValue, parseInt(message.client_id.stringValue)]
    })
  } else {
    await client.query({
      text: "INSERT INTO events (value, metric, user_name, client_id, public) VALUES ($1,$2,$3,$4,$5)",
      values: [parseFloat(message.value.stringValue), message.metric.stringValue, message.user_name.stringValue, parseInt(message.client_id.stringValue), false]
    })
  }

  await client.end();
};