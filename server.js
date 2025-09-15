
const express = require('express');
const http = require('http');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');
const { Client } = require('pg');
const axios = require('axios');
const { Parser } = require('node-sql-parser');
const dotenv = require('dotenv');

dotenv.config();
const port = 3000;
const app = express();
const server = http.createServer(app);
app.use(cors());

const wss = new WebSocket.Server({ server });
wss.on('connection', ws => console.log('Frontend connected for real-time updates!'));

const dbConfig = {
    user: 'postgres',
    host: 'localhost',
    database: 'MyCompany',
    password: process.env.PASSWORD,
    port: 5432,
};

console.log("DEBUG: Password loaded from .env:", process.env.PASSWORD ? "Yes" : "No, it's UNDEFINED!");
// --- 🔼 YE LINE ADD KARO 🔼 ---

const logDirectoryPath = "C:\\Program Files\\PostgreSQL\\17\\data\\log";
const AI_SERVER_URL = 'http://localhost:8000/analyze-query-gemini';

function broadcast(data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

async function getTableSchema(tableName) {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const res = await client.query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`, [tableName]);
        return JSON.stringify(res.rows);
    } catch (e) {
        console.error(" Could not fetch table schema for:", tableName, e.message);
        return "{}";
    } finally {
        if (client) await client.end();
    }
}

//API ENDPOINTS
app.get('/issues/next', async (req, res) => {
    console.log("➡️ Frontend is fetching the single next issue...");
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(
            "SELECT * FROM issues WHERE status = 'unsolved' ORDER BY last_seen DESC LIMIT 1"
        );
        res.json(result.rows.length > 0 ? result.rows[0] : null);
    } catch (err) {
        console.error(" Error fetching next issue:", err);
        res.status(500).json({ error: "Could not fetch next issue." });
    } finally {
        if (client) await client.end();
    }
});

app.post('/issues/:hash/solve', async (req, res) => {
    const { hash } = req.params;
    console.log(` Marking issue ${hash} as solved...`);
    const client = new Client(dbConfig);
    try {
        await client.connect();
        await client.query("UPDATE issues SET status = 'solved' WHERE query_hash = $1", [hash]);
        res.json({ message: "Issue marked as solved." });
        broadcast({ message: 'issue_solved_fetch_next' });
    } catch (err) {
        console.error(" Error updating issue status:", err);
        res.status(500).json({ error: "Could not update status." });
    } finally {
        if (client) await client.end();
    }
});



async function findAndSyncIssues(newData) {
    console.log("DEBUG #1: findAndSyncIssues function chala."); // DEBUG
    const client = new Client(dbConfig);
    const parser = new Parser();
    try {
        await client.connect();
        console.log("DEBUG #2: Database se connection safal hua."); // DEBUG

        const lines = newData.split('\n');
        // ... (multi-line logic to create logEntries)
        let logEntries = [];
        let currentEntry = null;
        for (const line of lines) {
            if (line.includes('duration:') && line.includes('statement:')) {
                if (currentEntry) logEntries.push(currentEntry);
                const parts = line.split('statement:');
                currentEntry = { header: parts[0], queryLines: [parts[1]] };
            } else if (currentEntry) {
                currentEntry.queryLines.push(line);
            }
        }
        if (currentEntry) logEntries.push(currentEntry);
        
        console.log(`DEBUG #3: Log file se ${logEntries.length} complete entries mili.`); // DEBUG

        for (const entry of logEntries) {
            const query = entry.queryLines.join('\n').trim();
            if (!query) continue;

            const durationMatch = entry.header.match(/duration: ([\d\.]+) ms/);
            if (durationMatch) {
                const durationInMs = parseFloat(durationMatch[1]);
                console.log(`DEBUG #4: Query mili -> "${query.substring(0, 30)}...", Duration: ${durationInMs}ms`); // DEBUG

                if (durationInMs > 0) {
                    console.log("DEBUG #5: Query slow hai. Aage process kar raha hoon..."); // DEBUG
                    
                    const queryHash = crypto.createHash('md5').update(query).digest('hex');
                    const { rows } = await client.query("SELECT query_hash FROM issues WHERE query_hash = $1", [queryHash]);
                    
                    if (rows.length === 0) {
                        console.log("DEBUG #6: Ye ek NAYI issue hai. DB mein INSERT karne jaa raha hoon..."); // DEBUG
                        // ... (baaki ka AI waala logic same rahega)
                        let tableName = null;
                        try {
                            const ast = parser.astify(query);
                            if (ast.from && ast.from.length > 0) tableName = ast.from[0].table;
                        } catch (e) { continue; }
                        if (!tableName) continue;

                        const tableSchema = await getTableSchema(tableName);
                        const aiResponse = await axios.post(AI_SERVER_URL, {
                            sql_query: query,
                            execution_time_sec: durationInMs / 1000,
                            table_schema: tableSchema
                        });
                        const { summary, recommendation, optimized_query } = aiResponse.data;
                        await client.query(
                            `INSERT INTO issues (query_hash, query_text, status, execution_time_sec, summary, recommendation, optimized_query) 
                             VALUES ($1, $2, 'unsolved', $3, $4, $5, $6)`,
                            [queryHash, query, durationInMs / 1000, summary, Array.isArray(recommendation) ? recommendation.join(' ') : recommendation, optimized_query]
                        );
                        console.log("DEBUG #7: Nayi issue DB mein INSERT ho gayi!"); // DEBUG
                    } else {
                        console.log("DEBUG #6: Ye issue purani hai. Sirf time update kar raha hoon."); // DEBUG
                        await client.query("UPDATE issues SET last_seen = CURRENT_TIMESTAMP, status = 'unsolved' WHERE query_hash = $1", [queryHash]);
                    }
                }
            }
        }
    } catch (err) {
        // Poora error object print karo
        console.error("❌ DEBUG: Function mein ERROR aaya. Poori detail:", err);
    } finally {
        if (client) await client.end();
        console.log("DEBUG #8: Function ne apna kaam poora kiya."); // DEBUG
    }
}

function watchLogFile() {
    console.log(` Watching for changes in: ${logDirectoryPath}`);
    let lastSize = 0;
    let lastFile = '';
    const checkFile = (filename) => {
        const fullLogPath = path.join(logDirectoryPath, filename);
        try {
            const stats = fs.statSync(fullLogPath);
            if (lastFile !== filename) lastSize = 0;
            if (stats.size > lastSize) {
                const newData = fs.readFileSync(fullLogPath, 'utf8').substring(lastSize);
                findAndSyncIssues(newData);
                lastSize = stats.size;
                lastFile = filename;
            }
        } catch (e) { /* Ignore errors */ }
    };
    setInterval(() => {
        try {
            const files = fs.readdirSync(logDirectoryPath);
            if (files.length > 0) {
                const newestFile = files.sort((a, b) => {
                     return fs.statSync(path.join(logDirectoryPath, b)).mtime.getTime() -
                            fs.statSync(path.join(logDirectoryPath, a)).mtime.getTime();
                })[0];
                checkFile(newestFile);
            }
        } catch(err) {
            console.error(" Error reading log directory:", err.message);
        }
    }, 2000);
}

server.listen(port, () => {
    console.log(` Real-time server is live on http://localhost:${port}`);
    console.log("Starting initial log sync...");
    try {
        const files = fs.readdirSync(logDirectoryPath);
        if (files.length > 0) {
            const newestFile = files.sort((a,b) => fs.statSync(path.join(logDirectoryPath, b)).mtime.getTime() - fs.statSync(path.join(logDirectoryPath, a)).mtime.getTime())[0];
            const fullData = fs.readFileSync(path.join(logDirectoryPath, newestFile), 'utf8');
            findAndSyncIssues(fullData).then(() => {
                console.log("Initial sync complete. Starting real-time watcher...");
                watchLogFile();
            });
        } else {
             console.log("No log file found for initial sync. Starting real-time watcher...");
            watchLogFile();
        }
    } catch (err) {
        console.error("Could not perform initial log sync:", err.message);
        watchLogFile();
    }
});