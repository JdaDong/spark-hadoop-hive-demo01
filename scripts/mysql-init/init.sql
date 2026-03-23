-- ============================================================
-- MySQL 初始化脚本: 电商数据库
-- 用于 Flink CDC 实时同步测试
-- ============================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS ecommerce DEFAULT CHARACTER SET utf8mb4;
USE ecommerce;

-- ========== 用户表 ==========
CREATE TABLE IF NOT EXISTS user_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    gender TINYINT COMMENT '0-未知 1-男 2-女',
    birthday DATE,
    province_id INT,
    city VARCHAR(50),
    register_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_login_time DATETIME,
    status TINYINT DEFAULT 1 COMMENT '0-禁用 1-正常',
    INDEX idx_province (province_id),
    INDEX idx_register_time (register_time)
) ENGINE=InnoDB COMMENT='用户信息表';

-- ========== 商品表 ==========
CREATE TABLE IF NOT EXISTS product_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock INT DEFAULT 0,
    description TEXT,
    status TINYINT DEFAULT 1 COMMENT '0-下架 1-上架',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_brand (brand)
) ENGINE=InnoDB COMMENT='商品信息表';

-- ========== 地区表 ==========
CREATE TABLE IF NOT EXISTS region_info (
    id INT PRIMARY KEY AUTO_INCREMENT,
    province VARCHAR(50) NOT NULL,
    city VARCHAR(50)
) ENGINE=InnoDB COMMENT='地区信息表';

-- ========== 订单表 ==========
CREATE TABLE IF NOT EXISTS order_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    province_id INT,
    order_status VARCHAR(20) DEFAULT 'created' COMMENT 'created/paid/shipped/completed/cancelled',
    total_amount DECIMAL(16, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    pay_amount DECIMAL(16, 2),
    pay_type VARCHAR(20) COMMENT 'alipay/wechat/bank',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_product (product_id),
    INDEX idx_create_time (create_time),
    INDEX idx_status (order_status)
) ENGINE=InnoDB COMMENT='订单信息表';

-- ========== 支付表 ==========
CREATE TABLE IF NOT EXISTS payment_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    pay_amount DECIMAL(16, 2) NOT NULL,
    pay_type VARCHAR(20) NOT NULL,
    pay_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'success',
    INDEX idx_order (order_id),
    INDEX idx_user (user_id),
    INDEX idx_pay_time (pay_time)
) ENGINE=InnoDB COMMENT='支付信息表';

-- ========== 插入测试数据 ==========

-- 地区数据
INSERT INTO region_info (province, city) VALUES
('北京', '北京'), ('上海', '上海'), ('广东', '广州'), ('广东', '深圳'),
('浙江', '杭州'), ('浙江', '宁波'), ('江苏', '南京'), ('江苏', '苏州'),
('四川', '成都'), ('湖北', '武汉'), ('湖南', '长沙'), ('福建', '厦门'),
('山东', '济南'), ('山东', '青岛'), ('河南', '郑州'), ('陕西', '西安');

-- 商品数据
INSERT INTO product_info (product_name, category, brand, price, stock) VALUES
('iPhone 15 Pro', '手机', 'Apple', 8999.00, 1000),
('MacBook Pro 14', '笔记本', 'Apple', 14999.00, 500),
('iPad Air', '平板', 'Apple', 4799.00, 800),
('AirPods Pro', '耳机', 'Apple', 1899.00, 2000),
('Galaxy S24 Ultra', '手机', 'Samsung', 9999.00, 600),
('ThinkPad X1 Carbon', '笔记本', 'Lenovo', 9999.00, 400),
('小米14 Pro', '手机', 'Xiaomi', 4999.00, 1500),
('华为 Mate 60 Pro', '手机', 'Huawei', 6999.00, 800),
('OPPO Find X7', '手机', 'OPPO', 4499.00, 1200),
('vivo X100 Pro', '手机', 'vivo', 4999.00, 1000);

-- 用户数据 (50个)
INSERT INTO user_info (username, email, gender, province_id, register_time) VALUES
('user001', 'user001@example.com', 1, 1, '2024-01-15 10:30:00'),
('user002', 'user002@example.com', 2, 2, '2024-01-16 11:00:00'),
('user003', 'user003@example.com', 1, 3, '2024-02-01 09:00:00'),
('user004', 'user004@example.com', 2, 4, '2024-02-10 14:30:00'),
('user005', 'user005@example.com', 1, 5, '2024-03-01 08:00:00'),
('user006', 'user006@example.com', 2, 6, '2024-03-15 16:00:00'),
('user007', 'user007@example.com', 1, 7, '2024-04-01 10:00:00'),
('user008', 'user008@example.com', 0, 8, '2024-04-10 12:00:00'),
('user009', 'user009@example.com', 1, 9, '2024-05-01 09:30:00'),
('user010', 'user010@example.com', 2, 10, '2024-05-15 15:00:00');

-- 订单测试数据
INSERT INTO order_info (user_id, product_id, province_id, order_status, total_amount, pay_amount, pay_type) VALUES
(1, 1, 1, 'completed', 8999.00, 8999.00, 'alipay'),
(2, 3, 2, 'completed', 4799.00, 4799.00, 'wechat'),
(3, 7, 3, 'paid', 4999.00, 4999.00, 'alipay'),
(4, 2, 4, 'shipped', 14999.00, 14999.00, 'bank'),
(5, 8, 5, 'created', 6999.00, NULL, NULL),
(1, 4, 1, 'completed', 1899.00, 1899.00, 'wechat'),
(6, 5, 6, 'paid', 9999.00, 9999.00, 'alipay'),
(7, 9, 7, 'created', 4499.00, NULL, NULL),
(8, 10, 8, 'completed', 4999.00, 4999.00, 'alipay'),
(9, 6, 9, 'cancelled', 9999.00, NULL, NULL);

-- 支付数据
INSERT INTO payment_info (order_id, user_id, pay_amount, pay_type) VALUES
(1, 1, 8999.00, 'alipay'),
(2, 2, 4799.00, 'wechat'),
(3, 3, 4999.00, 'alipay'),
(4, 4, 14999.00, 'bank'),
(6, 1, 1899.00, 'wechat'),
(7, 6, 9999.00, 'alipay'),
(9, 8, 4999.00, 'alipay');

-- ========== 授权 CDC 用户 ==========
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

SELECT 'MySQL 初始化完成!' AS message;
