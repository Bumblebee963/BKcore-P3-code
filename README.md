🤖 AI-Powered Database Profiler
This project is a real-time, AI-driven observability tool that automatically detects slow SQL queries, analyzes them using a powerful generative AI model, and provides actionable recommendations for optimization.

It monitors a PostgreSQL database, captures slow queries, and presents them one at a time in a clean web interface for a database administrator (DBA) to review and resolve.


✨ Core Features
Real-Time Monitoring: Actively watches PostgreSQL log files to detect slow queries as they happen.

Intelligent Analysis: Sends slow queries, execution metrics, and table schemas to a Python-based AI service powered by the Gemini API for in-depth analysis.

Actionable Recommendations: The AI provides a simple summary of the problem, a clear recommendation (like creating an index), and an optimized version of the query.

Stateful Issue Tracking: Uses a PostgreSQL database to keep track of all identified issues and their status (unsolved, solved), preventing duplicate alerts.

Interactive Queue UI: A clean, web-based frontend that displays only one unsolved issue at a time, allowing an administrator to focus and resolve problems sequentially.

Microservice Architecture: Built with a Node.js server that acts as a manager/orchestrator and a separate Python (FastAPI) server that serves as the AI brain.

⚙️ How It Works
The system follows a modern, event-driven microservice architecture:

Detection (Node.js): A Node.js server continuously monitors the PostgreSQL log directory. When a new query is logged that exceeds a predefined time threshold (e.g., >1000ms), it's identified as a potential issue.

Enrichment (Node.js): The Node.js server parses the query to identify the table(s) involved and connects to the database to fetch the relevant table schemas.

Delegation (Node.js → Python): The query, execution time, and schema are sent via an API call to a Python FastAPI server.

Analysis (Python): The Python server formats the data into a detailed prompt and sends it to the Gemini AI model, requesting a JSON response containing a summary, recommendation, and optimized query.

Storage (Node.js → PostgreSQL): The Node.js server receives the AI's analysis and stores the complete issue (query, analysis, status, etc.) in a dedicated issues table in the project's PostgreSQL database.

Notification (WebSocket): After storing a new issue, the Node.js server broadcasts a WebSocket signal to all connected web clients, alerting them that a new issue is available.

Display (Frontend): The frontend, upon receiving the signal, makes an API call to the Node.js server to fetch the single most recent unsolved issue from the database and displays it in a clean, actionable card format.

Resolution: When the user clicks "Mark as Solved," the frontend notifies the backend, which updates the issue's status in the database and sends a new WebSocket signal to the frontend, triggering it to fetch the next unsolved issue.

🛠️ Tech Stack
Backend (Manager/Orchestrator): Node.js, Express.js, WebSocket (ws), Axios, node-sql-parser, pg

Backend (AI Brain): Python, FastAPI, Google Generative AI (gemini)

Database: PostgreSQL

Frontend: HTML, CSS, JavaScript (using fetch and WebSocket APIs)

🚀 Getting Started
Prerequisites
Node.js and npm

Python and pip

A running PostgreSQL instance

A Google AI (Gemini) API Key

Setup
Clone the Repository:

Bash

git clone [your-repo-url]
cd [your-repo-name]
Backend Setup (Node.js):

Navigate to the server.js directory.

Install dependencies: npm install express cors pg axios ws node-sql-parser

Set up your database and create the issues table using the provided SQL schema.

Configure your database credentials in the dbConfig object in server.js.

Configure your PostgreSQL log_min_duration_statement setting to log slow queries.

AI Brain Setup (Python):

Navigate to the main.py directory.

Install dependencies: pip install "fastapi[all]" google-generativeai python-dotenv

Create a .env file and add your GOOGLE_API_KEY.

Running the System:

Terminal 1 (Start the AI Brain):

Bash

uvicorn main:app --reload
Terminal 2 (Start the Node.js Manager):

Bash

node server.js
Frontend: Open the index.html file in your web browser.

Now, run slow queries on your PostgreSQL database and watch them appear in the dashboard for analysis!