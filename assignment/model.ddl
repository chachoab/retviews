CREATE TABLE `clothes` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `title_id` int,
  `brand_id` int,
  `material_id` int
);

CREATE TABLE `clothes_attributes` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `clothes_id` int,
  `color_id` int
);

CREATE TABLE `price_listing` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `clothes_attributes_id` int,
  `price` decimal,
  `date` date
);

CREATE TABLE `title` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `title` varchar(255)
);

CREATE TABLE `brand` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `brand` varchar(255)
);

CREATE TABLE `material` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `material` varchar(255)
);

CREATE TABLE `color` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `color` varchar(255)
);

ALTER TABLE `clothes` ADD FOREIGN KEY (`title_id`) REFERENCES `title` (`id`);

ALTER TABLE `clothes` ADD FOREIGN KEY (`brand_id`) REFERENCES `brand` (`id`);

ALTER TABLE `clothes` ADD FOREIGN KEY (`material_id`) REFERENCES `material` (`id`);

ALTER TABLE `clothes_attributes` ADD FOREIGN KEY (`color_id`) REFERENCES `color` (`id`);

ALTER TABLE `clothes_attributes` ADD FOREIGN KEY (`clothes_id`) REFERENCES `clothes` (`id`);

ALTER TABLE `price_listing` ADD FOREIGN KEY (`clothes_attributes_id`) REFERENCES `clothes_attributes` (`id`);

INSERT INTO title (title) VALUES ('title_a'), ('title_b'), ('Sleeveless Shirt'), ('Baggy Trousers');
INSERT INTO brand (brand) VALUES ('brand_a'), ('brand_b'), ('brand_c'), ('Zara');
INSERT INTO material (material) VALUES ('material_a'), ('material_b'), ('material_c');
INSERT INTO color (color) VALUES ('color_a'), ('color_b'), ('color_c'), ('color_d');

INSERT INTO clothes (title_id, brand_id, material_id) VALUES (1, 2, 1);
INSERT INTO clothes (title_id, brand_id, material_id) VALUES (2, 1, 2);
INSERT INTO clothes (title_id, brand_id, material_id) VALUES (3, 4, 3);
INSERT INTO clothes (title_id, brand_id, material_id) VALUES (4, 3, 3);
INSERT INTO clothes (title_id, brand_id, material_id) VALUES (3, 4, 1);
INSERT INTO clothes (title_id, brand_id, material_id) VALUES (3, 4, 1);
INSERT INTO clothes (title_id, brand_id, material_id) VALUES (3, 4, 1);
INSERT INTO clothes (title_id, brand_id, material_id) VALUES (4, 1, 2);

INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (1, 1);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (1, 2);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (2, 3);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (2, 4);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (3, 1);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (3, 2);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (4, 3);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (4, 4);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (5, 1);
INSERT INTO clothes_attributes (clothes_id, color_id) VALUES (5, 2);

INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 1, STR_TO_DATE('1-01-2012', '%d-%m-%Y'));
INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 2, STR_TO_DATE('1-01-2020', '%d-%m-%Y'));
INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 3, STR_TO_DATE('1-01-2021', '%d-%m-%Y'));

INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 4, STR_TO_DATE('1-01-2012', '%d-%m-%Y'));
INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 5, STR_TO_DATE('1-01-2020', '%d-%m-%Y'));
INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 6, STR_TO_DATE('1-01-2021', '%d-%m-%Y'));

INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 7, STR_TO_DATE('1-01-2012', '%d-%m-%Y'));
INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 8, STR_TO_DATE('1-01-2020', '%d-%m-%Y'));
INSERT INTO price_listing (clothes_attributes_id, price, date) VALUES (5, 9, STR_TO_DATE('1-01-2021', '%d-%m-%Y'));