SELECT 
    *
FROM
    world_layoffs.layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

CREATE TABLE layoffs_staging LIKE Layoffs;

SELECT 
    *
FROM
    Layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM Layoffs;

-- 1. Remove Duplicates

SELECT 
    *
FROM
    layoffs_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,percentage_laid_off, `date`) AS row_num
FROM world_layoffs.layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

-- Just checking that these are duplicates
SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'Oda';

-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

-- these are the ones we want to delete!

CREATE TABLE `layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

SELECT 
    *
FROM
    layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    row_num > 1;

-- These are our duplicates that we need to delete

DELETE FROM layoffs_staging2 
WHERE
    row_num > 1;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    row_num > 1;

SELECT 
    *
FROM
    layoffs_staging2;

-- Now to standardise the data

SELECT DISTINCT
    (company)
FROM
    layoffs_staging2;

UPDATE layoffs_staging2 
SET 
    company = TRIM(company);

SELECT DISTINCT
    (industry)
FROM
    layoffs_staging2
ORDER BY 1;

-- Crypto Currency should be 'crypto'

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry LIKE 'crypto%';

UPDATE layoffs_staging2 
SET 
    industry = 'Crytpo'
WHERE
    industry = 'Crytpo Currency';

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry = 'crypto%';

-- 'Crypto Currency changed to 'Crypto'

SELECT DISTINCT
    country
FROM
    layoffs_staging2
ORDER BY country ASC;

-- 2x United States. One has period at end. 

SELECT DISTINCT
    country
FROM
    layoffs_staging2
WHERE
    country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM country)
FROM
    layoffs_staging2
WHERE
    country LIKE 'United States%'
ORDER BY 1;

UPDATE layoffs_staging2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'United States%';


SELECT DISTINCT
    country
FROM
    layoffs_staging2
WHERE
    country LIKE 'United States%';

-- Change date format and change column to a date calumn

SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    layoffs_staging2;

UPDATE layoffs_staging2 
SET 
    date = STR_TO_DATE(`date`, '%m/%d/%Y');

Alter table layoffs_staging2
modify column `date` Date;

-- Remove null and blank values

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry IS NULL OR industry = '';

-- check whether other companies have industry populated for the ones highlighted

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    company = 'airbnb';

-- yes so i will update this column to match

UPDATE layoffs_staging2 
SET 
    industry = NULL
WHERE
    industry = '';

UPDATE layoffs_staging2 t1
        JOIN
    layoffs_staging2 t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;

-- check...

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    company = 'airbnb';

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- this data where there is no total laid off or percentage laid off isnt worthwhile

DELETE FROM layoffs_staging2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

-- drop redundant columns 

Alter table
layoffs_staging2
drop column row_num;

-- final table for exploratory data analysis

SELECT 
    *
FROM
    layoffs_staging2;
