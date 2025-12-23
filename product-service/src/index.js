const express = require('express');
const mysql = require('mysql2/promise');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3002;
const SECRET_NAME = process.env.DB_SECRET_NAME || 'shopping-db-credentials';
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';

let dbPool = null;
const secretsClient = new SecretsManagerClient({ region: AWS_REGION });

async function getDbCredentials() {
  try {
    const command = new GetSecretValueCommand({ SecretId: SECRET_NAME });
    const data = await secretsClient.send(command);
    return JSON.parse(data.SecretString);
  } catch (error) {
    console.error('Error fetching secret:', error);
    throw error;
  }
}

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
    
    // Create products table
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        price DECIMAL(10, 2) NOT NULL,
        stock_quantity INT DEFAULT 0,
        category VARCHAR(100),
        image_url VARCHAR(500),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    
    console.log('Products table ready');
    return dbPool;
  } catch (error) {
    console.error('Database initialization error:', error);
    throw error;
  }
}

app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'product-service', timestamp: new Date().toISOString() });
});

// Create new product
app.post('/products', async (req, res) => {
  try {
    const { name, description, price, stock_quantity, category, image_url } = req.body;
    
    if (!name || !price) {
      return res.status(400).json({ error: 'Name and price are required' });
    }
    
    const pool = await initDatabase();
    
    const [result] = await pool.query(
      'INSERT INTO products (name, description, price, stock_quantity, category, image_url) VALUES (?, ?, ?, ?, ?, ?)',
      [name, description, price, stock_quantity || 0, category, image_url]
    );
    
    res.status(201).json({
      message: 'Product created successfully',
      product: {
        id: result.insertId,
        name,
        description,
        price,
        stock_quantity,
        category,
        image_url
      }
    });
  } catch (error) {
    console.error('Create product error:', error);
    res.status(500).json({ error: 'Failed to create product', details: error.message });
  }
});

// List all products with filtering
app.get('/products', async (req, res) => {
  try {
    const { category, min_price, max_price, search } = req.query;
    const pool = await initDatabase();
    
    let query = 'SELECT * FROM products WHERE 1=1';
    const params = [];
    
    if (category) {
      query += ' AND category = ?';
      params.push(category);
    }
    
    if (min_price) {
      query += ' AND price >= ?';
      params.push(parseFloat(min_price));
    }
    
    if (max_price) {
      query += ' AND price <= ?';
      params.push(parseFloat(max_price));
    }
    
    if (search) {
      query += ' AND (name LIKE ? OR description LIKE ?)';
      params.push(`%${search}%`, `%${search}%`);
    }
    
    query += ' ORDER BY created_at DESC';
    
    const [rows] = await pool.query(query, params);
    
    res.json({
      count: rows.length,
      products: rows
    });
  } catch (error) {
    console.error('List products error:', error);
    res.status(500).json({ error: 'Failed to fetch products', details: error.message });
  }
});

// Get product by ID
app.get('/products/:id', async (req, res) => {
  try {
    const pool = await initDatabase();
    const [rows] = await pool.query('SELECT * FROM products WHERE id = ?', [req.params.id]);
    
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    
    res.json(rows[0]);
  } catch (error) {
    console.error('Get product error:', error);
    res.status(500).json({ error: 'Failed to fetch product', details: error.message });
  }
});

// Update product
app.put('/products/:id', async (req, res) => {
  try {
    const { name, description, price, stock_quantity, category, image_url } = req.body;
    const pool = await initDatabase();
    
    const updates = [];
    const params = [];
    
    if (name !== undefined) { updates.push('name = ?'); params.push(name); }
    if (description !== undefined) { updates.push('description = ?'); params.push(description); }
    if (price !== undefined) { updates.push('price = ?'); params.push(price); }
    if (stock_quantity !== undefined) { updates.push('stock_quantity = ?'); params.push(stock_quantity); }
    if (category !== undefined) { updates.push('category = ?'); params.push(category); }
    if (image_url !== undefined) { updates.push('image_url = ?'); params.push(image_url); }
    
    if (updates.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }
    
    params.push(req.params.id);
    
    const [result] = await pool.query(
      `UPDATE products SET ${updates.join(', ')} WHERE id = ?`,
      params
    );
    
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    
    res.json({ message: 'Product updated successfully' });
  } catch (error) {
    console.error('Update product error:', error);
    res.status(500).json({ error: 'Failed to update product', details: error.message });
  }
});

// Delete product
app.delete('/products/:id', async (req, res) => {
  try {
    const pool = await initDatabase();
    const [result] = await pool.query('DELETE FROM products WHERE id = ?', [req.params.id]);
    
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    
    res.json({ message: 'Product deleted successfully' });
  } catch (error) {
    console.error('Delete product error:', error);
    res.status(500).json({ error: 'Failed to delete product', details: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Product Service running on port ${PORT}`);
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing database connections...');
  if (dbPool) await dbPool.end();
  process.exit(0);
});
