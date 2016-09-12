CREATE USER adequatecli WITH PASSWORD  '4dequat3';
CREATE DATABASE adequate;
GRANT ALL PRIVILEGES ON DATABASE adequate TO adequatecli;
ALTER DATABASE adequate OWNER TO adequatecli;
