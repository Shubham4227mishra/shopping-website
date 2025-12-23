const express = require('express');
const mysql = require('mysql2/promise');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3003;
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
    
    // Create carts table
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS carts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        status VARCHAR(50) DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
      )
    `);
    
    // Create cart_items table
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS cart_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        cart_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL DEFAULT 1,
        price_at_addition DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (cart_id) REFERENCES carts(id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
        UNIQUE KEY unique_cart_product (cart_id, product_id)
      )
    `);
    
    console.log('Cart tables ready');
    return dbPool;
  } catch (error) {
    console.error('Database initialization error:', error);
    throw error;
  }
}

app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'cart-service', timestamp: new Date().toISOString() });
});

// Create or get active cart for user
app.post('/cart/create', async (req, res) => {
  try {
    const { user_id } = req.body;
    
    if (!user_id) {
      return res.status(400).json({ error: 'user_id is required' });
    }
    
    const pool = await initDatabase();
    
    // Check if user exists
    const [users] = await pool.query('SELECT id FROM users WHERE id = ?', [user_id]);
    if (users.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Check for existing active cart
    const [carts] = await pool.query(
      'SELECT id FROM carts WHERE user_id = ? AND status = "active"',
      [user_id]
    );
    
    if (carts.length > 0) {
      return res.json({
        message: 'Active cart already exists',
        cart_id: carts[0].id
      });
    }
    
    // Create new cart
    const [result] = await pool.query(
      'INSERT INTO carts (user_id, status) VALUES (?, "active")',
      [user_id]
    );
    
    res.status(201).json({
      message: 'Cart created successfully',
      cart_id: result.insertId
    });
  } catch (error) {
    console.error('Create cart error:', error);
    res.status(500).json({ error: 'Failed to create cart', details: error.message });
  }
});

// Add item to cart
app.post('/cart/items', async (req, res) => {
  try {
    const { cart_id, product_id, quantity } = req.body;
    
    if (!cart_id || !product_id || !quantity) {
      return res.status(400).json({ error: 'cart_id, product_id, and quantity are required' });
    }
    
    if (quantity <= 0) {
      return res.status(400).json({ error: 'Quantity must be positive' });
    }
    
    const pool = await initDatabase();
    
    // Verify cart exists and is active
    const [carts] = await pool.query(
      'SELECT id, status FROM carts WHERE id = ?',
      [cart_id]
    );
    
    if (carts.length === 0) {
      return res.status(404).json({ error: 'Cart not found' });
    }
    
    if (carts[0].status !== 'active') {
      return res.status(400).json({ error: 'Cart is not active' });
    }
    
    // Get product details and verify stock
    const [products] = await pool.query(
      'SELECT id, name, price, stock_quantity FROM products WHERE id = ?',
      [product_id]
    );
    
    if (products.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    
    const product = products[0];
    
    if (product.stock_quantity < quantity) {
      return res.status(400).json({ 
        error: 'Insufficient stock',
        available: product.stock_quantity
      });
    }
    
    // Add or update cart item
    await pool.query(`
      INSERT INTO cart_items (cart_id, product_id, quantity, price_at_addition)
      VALUES (?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE 
        quantity = quantity + VALUES(quantity),
        price_at_addition = VALUES(price_at_addition)
    `, [cart_id, product_id, quantity, product.price]);
    
    res.status(201).json({
      message: 'Item added to cart successfully',
      product: {
        id: product.id,
        name: product.name,
        price: product.price,
        quantity
      }
    });
  } catch (error) {
    console.error('Add to cart error:', error);
    res.status(500).json({ error: 'Failed to add item to cart', details: error.message });
  }
});

// View cart with items
app.get('/cart/:cart_id', async (req, res) => {
  try {
    const pool = await initDatabase();
    
    // Get cart details
    const [carts] = await pool.query(
      'SELECT c.*, u.username FROM carts c JOIN users u ON c.user_id = u.id WHERE c.id = ?',
      [req.params.cart_id]
    );
    
    if (carts.length === 0) {
      return res.status(404).json({ error: 'Cart not found' });
    }
    
    // Get cart items with product details
    const [items] = await pool.query(`
      SELECT 
        ci.id,
        ci.quantity,
        ci.price_at_addition,
        p.id as product_id,
        p.name as product_name,
        p.description,
        p.stock_quantity,
        (ci.quantity * ci.price_at_addition) as subtotal
      FROM cart_items ci
      JOIN products p ON ci.product_id = p.id
      WHERE ci.cart_id = ?
    `, [req.params.cart_id]);
    
    const total = items.reduce((sum, item) => sum + parseFloat(item.subtotal), 0);
    
    res.json({
      cart: carts[0],
      items: items,
      item_count: items.length,
      total_amount: total.toFixed(2)
    });
  } catch (error) {
    console.error('View cart error:', error);
    res.status(500).json({ error: 'Failed to fetch cart', details: error.message });
  }
});

// Update item quantity in cart
app.put('/cart/items/:item_id', async (req, res) => {
  try {
    const { quantity } = req.body;
    
    if (!quantity || quantity <= 0) {
      return res.status(400).json({ error: 'Valid quantity is required' });
    }
    
    const pool = await initDatabase();
    
    const [result] = await pool.query(
      'UPDATE cart_items SET quantity = ? WHERE id = ?',
      [quantity, req.params.item_id]
    );
    
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Cart item not found' });
    }
    
    res.json({ message: 'Cart item updated successfully' });
  } catch (error) {
    console.error('Update cart item error:', error);
    res.status(500).json({ error: 'Failed to update cart item', details: error.message });
  }
});

// Remove item from cart
app.delete('/cart/items/:item_id', async (req, res) => {
  try {
    const pool = await initDatabase();
    
    const [result] = await pool.query(
      'DELETE FROM cart_items WHERE id = ?',
      [req.params.item_id]
    );
    
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Cart item not found' });
    }
    
    res.json({ message: 'Item removed from cart successfully' });
  } catch (error) {
    console.error('Remove cart item error:', error);
    res.status(500).json({ error: 'Failed to remove cart item', details: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Cart Service running on port ${PORT}`);
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing database connections...');
  if (dbPool) await dbPool.end();
  process.exit(0);
});
