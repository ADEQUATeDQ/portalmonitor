CREATE USER adequatebot WITH PASSWORD  '4dequat3';
CREATE DATABASE portalmonitor;
GRANT ALL PRIVILEGES ON DATABASE portalmonitor TO adequatebot;
ALTER DATABASE portalmonitor OWNER TO adequatebot;
