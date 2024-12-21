
## Process and Clean Data ## 

#clean up name
ALTER TABLE `iea global ev data 2024`
RENAME TO `ev_data`;

#look at data
SELECT * 
FROM ev_data;

# create staging table
CREATE TABLE `ev_data_staging` (
`region` text, 
`category` text, 
`parameter` text, 
`mode` text,
`powertrain` text,
`year` int, 
`unit` text, 
`value` double,
`row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# copy data with row to check for duplicates
INSERT INTO ev_data_staging
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY region, category, parameter, `mode`, powertrain, `year`, unit, `value`)
as row_num
FROM `ev_data`
);

# check for duplicates
SELECT * 
FROM ev_data_staging
WHERE row_num = 1;
# no duplicates found


#check for near duplicates
SELECT region, parameter, `mode`, `year`, unit, powertrain, COUNT(*) AS duplicate_count
FROM ev_data_staging
group by region, parameter, `mode`, `year`, unit, powertrain
HAVING duplicate_count > 1;

#1736 near duplicates found.  


#digging into these near duplicates
SELECT * 
FROM ev_data_staging
WHERE (region = "CHINA" AND parameter = "Electricity Demand" AND `mode` = "Buses");

#data between 2020 and 2023 from China, India, Europe and world is repeated 3 times under "Historical", "Projection-APS", "Projects-STEPS" is the 'catagory' column. Data after 2023 is repeated twice under parameters "Projection-APS", "Projects-STEPS"


# look for duplicated or near similar regions
SELECT DISTINCT region 
FROM ev_data_staging;

# none found


#look for NULL values 
SELECT * 
FROM ev_data_staging
WHERE region IS NULL OR category IS NULL OR parameter IS NULL OR `mode` IS NULL OR powertrain IS NULL OR `year` IS NULL OR unit IS NULL OR `value` IS NULL;

# none found


## Investigate Data ## 

SELECT * 
FROM ev_data_staging;

SELECT DISTINCT year 
FROM ev_data_staging;

SELECT DISTINCT unit 
FROM ev_data_staging;

SELECT DISTINCT category 
FROM ev_data_staging;

#looking into EV Stock Share of cars per country and assigning label
SELECT * 
FROM ev_data_staging
WHERE parameter = 'EV stock share' AND `mode` = 'Cars' AND region = 'United Kingdom';

SELECT
	region, 
    parameter, 
    `mode`,
    year,
    ROUND(`value`,1) AS `% of Stock`, 
	`value`,
	CASE
		WHEN `value` > 10 THEN 'Leading'
        WHEN `value` BETWEEN 2 AND 10  THEN 'Middle of the pack'
        WHEN `value` < 2 THEN 'Trailing'
	END AS `EV Car Saturation` 
    
FROM ev_data_staging
WHERE parameter = 'EV stock share' AND `mode` = 'Cars' AND `year` = 2023 AND category = 'Historical'
ORDER BY `value` DESC;



# finding the regions with the most charging points in 2023 
SELECT *
FROM ev_data_staging
WHERE parameter = 'EV charging points' AND `year` = 2023 AND region != 'World' AND category = 'Historical'
ORDER BY `value` desc;

# grouping by region
SELECT region, sum(`value`)  as total
FROM ev_data_staging
WHERE parameter = 'EV charging points' AND `year` = 2023 AND region != 'World' AND category = 'Historical'
GROUP BY region
ORDER BY total desc;

#what about the future?
SELECT region, sum(`value`) as '2035_total'
FROM ev_data_staging   
WHERE parameter = 'EV charging points' AND `year` = 2035 AND region != 'World' AND category = 'Projection-APS'
GROUP BY region
ORDER BY '2035_total' desc;

#table with 2020, 2025, 2030, and 2035 summed results
WITH CTE_Example AS (
SELECT region, 
	SUM(CASE WHEN parameter = 'EV charging points' AND `year` = 2020 AND region != 'World' AND category = 'Historical' THEN `value` END) AS Charging_Points_2020,
    SUM(CASE WHEN parameter = 'EV charging points' AND `year` = 2025 AND region != 'World' AND category = 'Projection-APS' THEN `value` END) AS Charging_Points_2025,
    SUM(CASE WHEN parameter = 'EV charging points' AND `year` = 2030 AND region != 'World' AND category = 'Projection-APS' THEN `value` END) AS Charging_Points_2030,
    SUM(CASE WHEN parameter = 'EV charging points' AND `year` = 2035 AND region != 'World' AND category = 'Projection-APS' THEN `value` END) AS Charging_Points_2035
FROM ev_data_staging
GROUP BY region
)
SELECT region,
		Charging_Points_2020, 
		Charging_Points_2025, 
        Charging_Points_2030, 
        Charging_Points_2035, 
        CASE WHEN Charging_Points_2020 IS NOT NULL THEN ROUND ((Charging_Points_2035 / Charging_Points_2020*100)-100,0) END AS Percentage_Increase_2020_to_2035
FROM CTE_Example
ORDER BY Percentage_Increase_2020_to_2035 DESC;

# India's growth projections for charging points from 2020 to 2035 are 549,196%  

#double check data
SELECT * 
FROM ev_data_staging
WHERE (region = "India" AND parameter = "EV charging points" AND (`year` = 2020 OR `year` = 2025 OR `year` = 2030 OR `year` = 2035));












































