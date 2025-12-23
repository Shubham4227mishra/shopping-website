const express = require('express');
const mysql = require('mysql2/promise');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;
const SECRET_NAME = process.env.DB_SECRET_NAME || 'shopping-db-credentials';
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';

let dbPool = null;

// Initialize Secrets Manager client
const secretsClient = new SecretsManagerClient({ region: AWS_REGION });

// Fetch database credentials from AWS Secrets Manager
async function getDbCredentials() {
  try {
    const command = new GetSecretValueCommand({ SecretId: SECRET_NAME });
    const data = await secretsClient.send(command);
    
    if (data.SecretString) {
      return JSON.parse(data.SecretString);
    }
    throw new Error('Secret not found');
  } catch (error) {
    console.error('Error fetching secret:', error);
    throw error;
  }
}

// Initialize database connection pool
async function initDatabase() {
  if (dbPool) return dbPool;
  
  try {
    const credentials = await getDbCredentials();
    
    dbPool = mysql.createPool({
      host: credentials.host,
      user: credentials.username,
      password: credentials.password,
      database: credentials.database || 'shopping_db',
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });
    
    console.log('Database pool created successfully');
    
    // Create users table if not exists
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(100) NOT NULL UNIQUE,
        email VARCHAR(255) NOT NULL UNIQUE,
        full_name VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    
    console.log('Users table ready');
    return dbPool;
  } catch (error) {
    console.error('Database initialization error:', error);
    throw error;
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'user-service', timestamp: new Date().toISOString() });
});

// Register new user
app.post('/users/register', async (req, res) => {
  try {
    const { username, email, full_name } = req.body;
    
    if (!username || !email) {
      return res.status(400).json({ error: 'Username and email are required' });
    }
    
    const pool = await initDatabase();
    
    const [result] = await pool.query(
      'INSERT INTO users (username, email, full_name) VALUES (?, ?, ?)',
      [username, email, full_name]
    );
    
    res.status(201).json({
      message: 'User registered successfully',
      user: {
        id: result.insertId,
        username,
        email,
        full_name
      }
    });
  } catch (error) {
    console.error('Registration error:', error);
    
    if (error.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'Username or email already exists' });
    }
    
    res.status(500).json({ error: 'Failed to register user', details: error.message });
  }
});

// List all users
app.get('/users', async (req, res) => {
  try {
    const pool = await initDatabase();
    const [rows] = await pool.query('SELECT id, username, email, full_name, created_at FROM users');
    
    res.json({
      count: rows.length,
      users: rows
    });
  } catch (error) {
    console.error('List users error:', error);
    res.status(500).json({ error: 'Failed to fetch users', details: error.message });
  }
});

// Get user by ID
app.get('/users/:id', async (req, res) => {
  try {
    const pool = await initDatabase();
    const [rows] = await pool.query(
      'SELECT id, username, email, full_name, created_at FROM users WHERE id = ?',
      [req.params.id]
    );
    
    if (rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    res.json(rows[0]);
  } catch (error) {
    console.error('Get user error:', error);
    res.status(500).json({ error: 'Failed to fetch user', details: error.message });
  }
});

// Update user
app.put('/users/:id', async (req, res) => {
  try {
    const { email, full_name } = req.body;
    const pool = await initDatabase();
    
    const [result] = await pool.query(
      'UPDATE users SET email = COALESCE(?, email), full_name = COALESCE(?, full_name) WHERE id = ?',
      [email, full_name, req.params.id]
    );
    
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    res.json({ message: 'User updated successfully' });
  } catch (error) {
    console.error('Update user error:', error);
    res.status(500).json({ error: 'Failed to update user', details: error.message });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`User Service running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing database connections...');
  if (dbPool) {
    await dbPool.end();
  }
  process.exit(0);
});
