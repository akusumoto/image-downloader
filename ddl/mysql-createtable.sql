CREATE TABLE IF NOT EXISTS imagedownloader.sites(
	id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	url TEXT
);
CREATE TABLE IF NOT EXISTS imagedownloader.pg_stats(
	id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	site_id integer,
	url text, 
	status integer
);
CREATE TABLE IF NOT EXISTS imagedownloader.img_stats(
	id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	site_id integer,
	url text,
       	status integer
);
CREATE TABLE IF NOT EXISTS imagedownloader.url_queue(
	id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	site_id integer,
       	url text
);

GRANT ALL ON imagedownloader.* TO 'imagedownloader'@'localhost' IDENTIFIED BY 'imagedownloader';
FLUSH PRIVILEGES;

CREATE INDEX imagedownloader.idx_pg_stats ON imagedownloader.pg_stats(site_id, url(255), status);
CREATE INDEX imagedownloader.idx_img_stats ON imagedownloader.img_stats(site_id, url(255), status);
CREATE INDEX imagedownloader.idx_url_queue ON imagedownloader.img_stats(site_id, url(255));
CREATE INDEX imagedownloader.idx2_url_queue ON imagedownloader.img_stats(site_id);

alter table img_stats convert to character set utf8;
alter table pg_stats convert to character set utf8;
alter table sites convert to character set utf8;
alter table url_queue convert to character set utf8;

ALTER TABLE url_queue ADD UNIQUE (site_id, url(255));
