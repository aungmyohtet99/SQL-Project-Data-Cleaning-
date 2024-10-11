-- Data Cleaning

-- 1. Remove Duplicate
-- 2. Standardize Data
-- 3. Null Values or blank values
-- 4. Remove Column

-- first,creating staging table to work in

Create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from
layoffs;

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

-- adding row_num column to remove duplicate

insert into layoffs_staging2
select *,
row_number() over(partition by
company,location,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

delete from
layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;

-- checking space

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

-- replace missing data

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- removing full stop (.) in the end

select distinct country,trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- formatting date cloumn

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- modifying data type of column

alter table layoffs_staging2
modify column `date` date;

-- replace blank to null in order to work easier

update layoffs_staging2
set industry = null
where industry = '';

select t1.company,t1.industry,
t2.company,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * from layoffs_staging2
where industry is null;

select *
from layoffs_staging2
where total_laid_off is null and
percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null and
percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

