const { Client } = require('pg');

exports.handler = async function (event, context, callback) {
  const message = event.Records[0].messageAttributes;
  const client = new Client();
  await client.connect();

  if (message.most) {
    const result = await client.query({
      text: "Select value FROM events WHERE metric = $1 AND user_name = $2 AND client_id = $3;",
      values: [message.metric.stringValue, message.user_name.stringValue, parseInt(message.client_id.stringValue)]
    })

    if (result.rows[0]) {
      if (result.rows[0].value < parseFloat(message.value.stringValue)) {
        await client.query({
          text: "UPDATE events SET value = $1 WHERE metric = $2 AND user_name = $3 AND client_id = $4;",
          values: [parseFloat(message.value.stringValue), message.metric.stringValue, message.user_name.stringValue, parseInt(message.client_id.stringValue)]
        })
      }
    } else {

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
    }

  } else {
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
  }
  await client.end();
};