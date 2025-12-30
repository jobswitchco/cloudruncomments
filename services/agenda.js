// agenda.js
import Agenda from "agenda";
const username = 'myhandlein_db_user';
const password = process.env.MONGO_DB_PASS;

var MONGO_URI = 'mongodb+srv://'+username+':'+password+'@cluster0.itfkrwb.mongodb.net/?appName=Cluster0';

const agenda = new Agenda({
  db: {
    address: MONGO_URI, // ✅ Your MongoDB connection string
    collection: "agenda_jobs",      // Collection where jobs will be stored
  },
  processEvery: "10 seconds", // How often it checks for due jobs
  maxConcurrency: 20,
  defaultConcurrency: 5,
});

export default agenda;
