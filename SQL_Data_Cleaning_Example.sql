-- Data cleaning


SELECT *
FROM layoffs

-- 1. Remove duplicates

CREATE TABLE layoffs_staging
LIKE Layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * ,
ROW_NUMBER () OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT * ,
ROW_NUMBER () OVER (PARTITION BY company, location,  industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num >1;

SELECT *
FROM layoffs_staging
WHERE company = 'casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER () OVER (PARTITION BY company, location,  industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

DELETE
FROM layoffs_staging2
WHERE row_num >1;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

SET sql_safe_updates=0;

SELECT *
FROM layoffs_staging2

-- 2. Standardise the data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT industry 
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT country, TRIM(TRAILING'.' FROM Country)
FROM layoffs_staging2
ORDER by 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING'.' FROM Country)
WHERE country LIKE 'United States$';

SELECT *
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null values or balnk values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
    WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL;
    
    UPDATE layoffs_staging2 t1
    JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    SET t1.company = t2.company
    WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL;
    
    UPDATE layoffs_staging2
    SET industry = NULL 
    WHERE industry = '';
    
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- 4 Remove any columns

ALTER TABLE layoffs_staging2
  DROP COLUMN row_num;