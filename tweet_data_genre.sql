with first as(
  SELECT 
    large_class_ip_name,
    min(tweet_date) product_date
  FROM `hogeticlab-legs-prd.dataform.dmart_daily_ip_tweet_category` 
  where cnt_goods >= 1 and cnt_campaign<1
  group by large_class_ip_name
),
difference as(
  select
    a.large_class_ip_name,
    b.product_date,
    a.tweet_date,
    date_diff(a.tweet_date, b.product_date, month) gap, 
    a.tweet_genre,
    a.cnt_fav_rt,
    a.cnt_goods,
    a.cnt_campaign
  from `hogeticlab-legs-prd.dataform.dmart_daily_ip_tweet_category` a
  left join first b
  on a.large_class_ip_name = b.large_class_ip_name
),
product_after_median as(
  select
    large_class_ip_name,
    product_date,
    percentile_cont(cnt_fav_rt, 0.5) OVER (PARTITION BY large_class_ip_name) median
  from difference
  where gap>0 and gap<=3 and cnt_goods>=1 and cnt_campaign<1
),
product_after_mean as(
  select 
      large_class_ip_name,
      count(*) cnt,
      avg(cnt_fav_rt) mean,
  from difference
  where gap>0 and gap<=3 and cnt_goods>=1 and cnt_campaign<1
  group by large_class_ip_name
),
normal_3before_mean as(
  select
    large_class_ip_name,
    count(*) cnt,
    avg(cnt_fav_rt) mean,
  from difference
  where gap<0 and gap>=-3 and cnt_goods=0 and cnt_campaign<1
  group by large_class_ip_name
),
normal_3before_median as(
  select
    large_class_ip_name,
    percentile_cont(cnt_fav_rt, 0.5) OVER (PARTITION BY large_class_ip_name) median
  from difference
  where gap<0 and gap>=-3 and cnt_goods=0 and cnt_campaign<1
),
normal_6before_median as(
  select
    large_class_ip_name,
    percentile_cont(cnt_fav_rt, 0.5) OVER (PARTITION BY large_class_ip_name) median
  from difference
  where gap<=-6 and gap>=-9 and cnt_goods=0 and cnt_campaign<1
),
normal_6before_mean as(
  select
    large_class_ip_name,
    count(*) cnt,
    avg(cnt_fav_rt) mean,
  from difference
  where gap<=-6 and gap>=-9 and cnt_goods=0 and cnt_campaign<1
  group by large_class_ip_name
),
product_after as(
  select 
    nb.large_class_ip_name,
    nm.product_date,
    nb.cnt, 
    nb.mean,
    nm.median
  from product_after_mean nb
  left join product_after_median nm
  on nb.large_class_ip_name = nm.large_class_ip_name
),
normal_3before as(
  select 
    nb.large_class_ip_name,
    nb.mean,
    nb.cnt,
    nm.median
  from normal_3before_mean nb
  left join normal_3before_median nm
  on nb.large_class_ip_name = nm.large_class_ip_name
),
normal_6before as(
  select 
    nb.large_class_ip_name,
    nb.mean,
    nb.cnt,
    nm.median
  from normal_6before_mean nb
  left join normal_6before_median nm
  on nb.large_class_ip_name = nm.large_class_ip_name
),
base as(
  select 
    pa.large_class_ip_name,
    pa.product_date, 
    pa.cnt product_count,
    pa.mean product_mean,
    pa.median product_median,
    n3.cnt three_month_count,
    n3.mean three_month_mean,
    n3.median three_month_median,
    n6.cnt six_month_count,
    n6.mean six_month_mean,
    n6.median six_month_median
  from product_after pa
  left join normal_3before n3
  on pa.large_class_ip_name = n3.large_class_ip_name
  left join normal_6before n6
  on pa.large_class_ip_name = n6.large_class_ip_name  
), summary as(
select distinct *
from base
where three_month_mean is not null and three_month_median >0
),
category as (
  select 
    su.*, 
    ge.event_content, 
  from summary as su
  left join `hogeticlab-legs-prd.dataform.dmart_ip_genre` ge
  on su.large_class_ip_name = ge.IP
)
select distinct *
from category
where event_content is not null and (event_content = 'ドラマ/青春' or event_content = 'SF/ファンタジー' or event_content = '恋愛/ラブコメ' or event_content = 'コメディ/ギャグ' or event_content ='アクション/バトル')
