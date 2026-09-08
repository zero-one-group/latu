SELECT sum(id) AS total, avg(id) AS mean, count(DISTINCT id % 3) AS buckets FROM range(10)
