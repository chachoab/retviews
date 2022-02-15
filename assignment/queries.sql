
SELECT DISTINCT brand.brand
FROM clothes
INNER JOIN title ON clothes.title_id = title.id
INNER JOIN brand ON clothes.brand_id = brand.id
WHERE title = 'Baggy Trousers';

SELECT MAX(lp.last_price)
FROM clothes
INNER JOIN title ON clothes.title_id = title.id
INNER JOIN brand ON clothes.brand_id = brand.id
INNER JOIN clothes_attributes ON clothes.id = clothes_attributes.clothes_id
INNER JOIN color ON clothes_attributes.color_id = color.id
INNER JOIN (
	SELECT 
        price_listing.clothes_attributes_id
		,price AS last_price
	FROM price_listing
	INNER JOIN (
		SELECT 
            clothes_attributes_id
			,MAX(DATE) AS max_date
		FROM price_listing
		GROUP BY clothes_attributes_id
		) md 
    ON price_listing.clothes_attributes_id = md.clothes_attributes_id
	AND price_listing.date = md.max_date
	) lp ON clothes_attributes.id = lp.clothes_attributes_id
WHERE 
    title = 'Sleeveless Shirt'
	AND brand = 'Zara';