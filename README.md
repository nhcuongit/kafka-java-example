# kafka-java-example

1. Tạo CSDL MySQL "banking" trên máy master:
```
CREATE USER 'kafka_user'@'master' IDENTIFIED BY 'Kafka@123';
GRANT ALL PRIVILEGES ON banking.* TO 'kafka_user'@'master' WITH GRANT OPTION;
FLUSH PRIVILEGES;
USE mysql;
UPDATE user SET host='%' WHERE user = 'kafka_user';

CREATE DATABASE IF NOT EXISTS banking;
USE banking;

CREATE TABLE IF NOT EXISTS customers (
    username VARCHAR(100),
    address VARCHAR(250)
);
INSERT INTO customers VALUES
	("nguyenvanquyet", "VIE"),
	("nguyenchithanh", "VIE"),
	("nguyenhungcuong", "VIE");

+-----------------+---------+
| username        | address |
+-----------------+---------+
| nguyenvanquyet  | VIE     |
| nguyenchithanh  | VIE     |
| nguyenhungcuong | VIE     |
+-----------------+---------+
```
