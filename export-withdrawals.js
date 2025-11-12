// export-withdrawals.js - simple CSV export for bookkeeping
require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('fs');

async function run() {
  const client = new MongoClient(process.env.MONGO_URI);
  await client.connect();
  const db = client.db(process.env.DB_NAME || 'tg_refbot');
  const rows = await db.collection('withdrawals').find().toArray();
  const csv = rows.map(r => `${r._id},${r.userId},${r.amount},${r.upi},${r.status},${r.requestedAt}`).join('\n');
  fs.writeFileSync('withdrawals.csv', 'id,userId,amount,upi,status,requestedAt\n' + csv);
  console.log('wrote withdrawals.csv');
  await client.close();
}
run().catch(err => console.error(err));
