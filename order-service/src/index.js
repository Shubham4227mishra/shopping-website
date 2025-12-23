const express = require('express');
const mysql = require('mysql2/promise');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3004;
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
    
    // Create orders table
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        cart_id INT NOT NULL,
        total_amount DECIMAL(10, 2) NOT NULL,
        status VARCHAR(50) DEFAULT 'pending',
        shipping_address TEXT,
        payment_method VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (cart_id) REFERENCES carts(id)
      )
    `);
    
    // Create order_items table
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS order_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        quantity INT NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        subtotal DECIMAL(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(id)
      )
    `);
    
    console.log('Order tables ready');
    return dbPool;
  } catch (error) {
    console.error('Database initialization error:', error);
    throw error;
  }
}

app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'order-service', timestamp: new Date().toISOString() });
});

// Place order from cart
app.post('/orders', async (req, res) => {
  let connection;
  
  try {
    const { user_id, cart_id, shipping_address, payment_method } = req.body;
    
    if (!user_id || !cart_id) {
      return res.status(400).json({ error: 'user_id and cart_id are required' });
    }
    
    const pool = await initDatabase();
    connection = await pool.getConnection();
    
    // Start transaction
    await connection.beginTransaction();
    
    // Verify user exists
    const [users] = await connection.query('SELECT id FROM users WHERE id = ?', [user_id]);
    if (users.length === 0) {
      await connection.rollback();
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Verify cart exists and is active
    const [carts] = await connection.query(
      'SELECT id, user_id, status FROM carts WHERE id = ? AND status = "active"',
      [cart_id]
    );
    
    if (carts.length === 0) {
      await connection.rollback();
      return res.status(404).json({ error: 'Cart not found or not active' });
    }
    
    if (carts[0].user_id !== user_id) {
      await connection.rollback();
      return res.status(403).json({ error: 'Cart does not belong to this user' });
    }
    
    // Get cart items
    const [cartItems] = await connection.query(`
      SELECT 
        ci.product_id,
        ci.quantity,
        ci.price_at_addition,
        p.name as product_name,
        p.stock_quantity,
        (ci.quantity * ci.price_at_addition) as subtotal
      FROM cart_items ci
      JOIN products p ON ci.product_id = p.id
      WHERE ci.cart_id = ?
    `, [cart_id]);
    
    if (cartItems.length === 0) {
      await connection.rollback();
      return res.status(400).json({ error: 'Cart is empty' });
    }
    
    // Verify stock availability for all items
    for (const item of cartItems) {
      if (item.stock_quantity < item.quantity) {
        await connection.rollback();
        return res.status(400).json({ 
          error: 'Insufficient stock',
          product: item.product_name,
          available: item.stock_quantity,
          requested: item.quantity
        });
      }
    }
    
    // Calculate total
    const totalAmount = cartItems.reduce((sum, item) => sum + parseFloat(item.subtotal), 0);
    
    // Create order
    const [orderResult] = await connection.query(
      'INSERT INTO orders (user_id, cart_id, total_amount, status, shipping_address, payment_method) VALUES (?, ?, ?, ?, ?, ?)',
      [user_id, cart_id, totalAmount, 'pending', shipping_address, payment_method]
    );
    
    const orderId = orderResult.insertId;
    
    // Insert order items and update stock
    for (const item of cartItems) {
      // Insert order item
      await connection.query(
        'INSERT INTO order_items (order_id, product_id, product_name, quantity, price, subtotal) VALUES (?, ?, ?, ?, ?, ?)',
        [orderId, item.product_id, item.product_name, item.quantity, item.price_at_addition, item.subtotal]
      );
      
      // Update product stock
      await connection.query(
        'UPDATE products SET stock_quantity = stock_quantity - ? WHERE id = ?',
        [item.quantity, item.product_id]
      );
    }
    
    // Update cart status
    await connection.query(
      'UPDATE carts SET status = "ordered" WHERE id = ?',
      [cart_id]
    );
    
    // Commit transaction
    await connection.commit();
    
    res.status(201).json({
      message: 'Order placed successfully',
      order: {
        id: orderId,
        user_id,
        cart_id,
        total_amount: totalAmount.toFixed(2),
        status: 'pending',
        items: cartItems.map(item => ({
          product_id: item.product_id,
          product_name: item.product_name,
          quantity: item.quantity,
          price: item.price_at_addition,
          subtotal: item.subtotal
        }))
      }
    });
  } catch (error) {
    if (connection) {
      await connection.rollback();
    }
    console.error('Place order error:', error);
    res.status(500).json({ error: 'Failed to place order', details: error.message });
  } finally {
    if (connection) {
      connection.release();
    }
  }
});

// Get order by ID
app.get('/orders/:id', async (req, res) => {
  try {
    const pool = await initDatabase();
    
    // Get order details
    const [orders] = await pool.query(`
      SELECT 
        o.*,
        u.username,
        u.email
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.id = ?
    `, [req.params.id]);
    
    if (orders.length === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }
    
    // Get order items
    const [items] = await pool.query(
      'SELECT * FROM order_items WHERE order_id = ?',
      [req.params.id]
    );
    
    res.json({
      order: orders[0],
      items: items,
      item_count: items.length
    });
  } catch (error) {
    console.error('Get order error:', error);
    res.status(500).json({ error: 'Failed to fetch order', details: error.message });
  }
});

// Get all orders for a user
app.get('/orders/user/:user_id', async (req, res) => {
  try {
    const pool = await initDatabase();
    
    const [orders] = await pool.query(`
      SELECT 
        o.id,
        o.total_amount,
        o.status,
        o.shipping_address,
        o.payment_method,
        o.created_at,
        COUNT(oi.id) as item_count
      FROM orders o
      LEFT JOIN order_items oi ON o.id = oi.order_id
      WHERE o.user_id = ?
      GROUP BY o.id
      ORDER BY o.created_at DESC
    `, [req.params.user_id]);
    
    res.json({
      count: orders.length,
      orders: orders
    });
  } catch (error) {
    console.error('Get user orders error:', error);
    res.status(500).json({ error: 'Failed to fetch orders', details: error.message });
  }
});

// Update order status
app.put('/orders/:id/status', async (req, res) => {
  try {
    const { status } = req.body;
    
    const validStatuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled'];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ error: 'Invalid status', valid_statuses: validStatuses });
    }
    
    const pool = await initDatabase();
    
    const [result] = await pool.query(
      'UPDATE orders SET status = ? WHERE id = ?',
      [status, req.params.id]
    );
    
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }
    
    res.json({ message: 'Order status updated successfully', status });
  } catch (error) {
    console.error('Update order status error:', error);
    res.status(500).json({ error: 'Failed to update order status', details: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Order Service running on port ${PORT}`);
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing database connections...');
  if (dbPool) await dbPool.end();
  process.exit(0);
});
