
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
import dotenv from "dotenv";

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
    const client = new Client(dbConfig);
    const parser = new Parser();
    try {
        await client.connect();
        const lines = newData.split('\n');
        let newIssueFound = false;
        for (const line of lines) {
            if (line.includes('duration:') && line.includes('statement:')) {
                const parts = line.split('statement:');
                if (parts.length < 2) continue;
                const query = parts[1].trim();
                if (!query) continue;
                const durationMatch = line.match(/duration: ([\d\.]+) ms/);
                if (durationMatch && parseFloat(durationMatch[1]) > 0) {
                    const durationInS = parseFloat(durationMatch[1]) / 1000;
                    const queryHash = crypto.createHash('md5').update(query).digest('hex');
                    const { rows } = await client.query("SELECT query_hash FROM issues WHERE query_hash = $1", [queryHash]);
                    if (rows.length === 0) {
                        let tableName = null;
                        try {
                            const ast = parser.astify(query);
                            if (ast.from && ast.from.length > 0) {
                                tableName = ast.from[0].table;
                            }
                        } catch (e) { continue; }
                        if (!tableName) continue;
                        console.log(`➕ New issue found for table '${tableName}'! Sending to AI...`);
                        const tableSchema = await getTableSchema(tableName);
                        const aiResponse = await axios.post(AI_SERVER_URL, {
                            sql_query: query,
                            execution_time_sec: durationInS,
                            table_schema: tableSchema
                        });
                        const { summary, recommendation, optimized_query } = aiResponse.data;
                        await client.query(
                            `INSERT INTO issues (query_hash, query_text, status, execution_time_sec, summary, recommendation, optimized_query) 
                             VALUES ($1, $2, 'unsolved', $3, $4, $5, $6)`,
                            [queryHash, query, durationInS, summary, Array.isArray(recommendation) ? recommendation.join(' ') : recommendation, optimized_query]
                        );
                        newIssueFound = true;
                    } else {
                        await client.query("UPDATE issues SET last_seen = CURRENT_TIMESTAMP, status = 'unsolved' WHERE query_hash = $1", [queryHash]);
                    }
                }
            }
        }
        if (newIssueFound) {
            broadcast({ message: 'new_issue_found' });
        }
    } catch (err) {
        console.error(" Error during log sync:", err.response ? err.response.data : err.message);
    } finally {
        if (client) await client.end();
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