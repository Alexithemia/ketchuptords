const { Client } = require('pg');

exports.handler = async function (event, context, callback) {
  const message = event.Records[0].messageAttributes;
  const client = new Client();
  await client.connect();

  await client.query(`CREATE TABLE IF NOT EXISTS ${message.key.stringValue} (id SERIAL PRIMARY KEY, user_id INT, metric CHAR(20) NOT NULL, value DECIMAL NOT NULL)`);

  await client.query(`INSERT INTO ${message.key.stringValue} (value, metric, user_id) VALUES (${parseFloat(message.value.stringValue)}, '${message.metric.stringValue}', ${parseInt(message.user_id.stringValue)})`);

  await client.end();
};