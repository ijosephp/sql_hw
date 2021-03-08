--Name: Immanuel Ponminissery
--Assignment 4
--Prof: Dr Tej Anand


--Q1
/*using full outer join*/
select count(distinct cc.cc_id) as total_content_creators,MIN(v.video_length) as min_vid_length, max(v.video_length) as max_video_length,max(v.views) as maximum_views
from video v
    full outer join content_creators cc
        on v.cc_id = cc.cc_id;
              
--Q2

select count(c.video_id) AS NUMBER_OF_USER_COMMENTS, max(c.time_date) as most_recent_comment_date, v.title
from comments c
inner join video v
    on c.video_id = v.video_id
group by c.video_id, v.title
order by most_recent_comment_date;

--Q3


select cc.city,count(cc.cc_id) as number_of_ccs,  round(avg(v.likes),2) as avg_likes
from content_creators cc
    inner join video v
        on cc.cc_id = v.cc_id
group by cc.city
order by avg_likes desc;

--Q4

select t.topic_id,t.topic_name,to_char(avg(TO_NUMBER(SUBSTR (V.video_size,  0,  LENGTH (V.video_size)-2))),'99.99') AS AVG_VID_SIZE_in_mb, sum(v.likes) as total_likes
from topic t
inner join video_topic_link vtl
on t.topic_id = vtl.topic_id
inner join video v
on vtl.video_id = v.video_id
group by t.topic_id, t.topic_name
order by topic_id;

--Q4b

select distinct t.topic_id,t.topic_name,to_char(avg(TO_NUMBER(SUBSTR (v.video_size,  0,  LENGTH (v.video_size)-2))) over (partition by t.topic_id),'99.99') AS AVG_VID_SIZE_in_mb, sum(v.likes) over (partition by t.topic_id) as total_likes  
from topic t
inner join video_topic_link vtl
on t.topic_id = vtl.topic_id
inner join video v
on vtl.video_id = v.video_id
order by topic_id;

--Q5

/*Not accounting for videos that have not generated more than 10 awards. Said videos are not included in the total sum of awards
earned. The question asked for one row for each user and not to include videos with less than 10 awards.*/

select first_name, last_name, sum(awards_earned) as awards_earned
from (
    select u.first_name, u.last_name, trunc(((v.views)-100)/5000) as awards_earned
    from video v
        inner join content_creators cc
            on v.cc_id = cc.cc_id
        inner join user_table u
            on cc.user_id = u.user_id
    where trunc(((v.views)-100)/5000)>=10
    order by awards_earned, last_name)
group by first_name, last_name
order by awards_earned desc,last_name;

--Q6

select first_name, city_billing, state_billing, sum(number_of_cards) as count_cards
from(
    select u.first_name, c.city_billing, c.state_billing,count(c.card_id) as number_of_cards
    from creditcard c
    inner join content_creators cc
        on c.contentcreator_id = cc.cc_id
    inner join user_table u
        on cc.user_id = u.user_id
    where c.state_billing = 'TX' or c.state_billing = 'NY'
    group by u.first_name,city_billing, state_billing
    order by city_billing)
group by rollup(state_billing,city_billing, first_name);

--Q6B

/*CUBE basically creates more combinations compared to rollup. In fact, when CUBE is used, subtotals are created for ALL 
possible combinations. From the reading, it seems like CUBE is faster in terms of generating summary tables and you also get 
more insight since more dimensions are being considered.*/

--Q7

/*using case command*/
select c.card_id,cc.street_address, c.street_billing, case when cc.street_address =  c.street_billing 
                                                                    then 'Y' else 'N' 
                                                                    end 
                                                                    as Flag
from content_creators cc
inner join creditcard c
    on cc.cc_id = c.contentcreator_id;

--Q8
/*using outer join*/
select v.cc_id,count(v.video_id) as number_of_vids, count(distinct vtl.topic_id) as unique_topics
from video v
full outer join video_topic_link vtl
    on v.video_id = vtl.video_id
group by v.cc_id
having count(distinct vtl.topic_id)>=2
order by cc_id desc;

--Q8b

/*using window. the question explicity asked to remove the 'at least 2' requirement*/

select distinct v.cc_id,count(v.video_id) over (partition by v.cc_id) as number_of_vids, count(distinct vtl.topic_id) over (partition by v.cc_id)as unique_topics
from video v
full outer join video_topic_link vtl
    on v.video_id = vtl.video_id
order by cc_id desc;

--SUBQUERY PROBLEMS

--Q9
/*subquery in where*/

select topic_name from topic
where topic_id in (select vtl.topic_id from video_topic_link vtl
                        inner join video v
                        on v.video_id = vtl.video_id)
ORDER BY topic_name DESC;

--Q10

/*subquery used to access the avg likes*/

SELECT cc.user_id, v.video_id, v.likes 
from video v 
join content_creators cc
    on cc.cc_id=v.cc_id
where likes > (select avg(likes) from video)
order by v.likes;

--Q11

/*this can be made more concise. but it makes use of multiple subqueries in where*/
select first_name, last_name, email, cc_flag, birthdate
from user_table
where user_id in (
    select user_id
    from content_creators
    where user_id in (
    select user_id 
    from user_table
    where user_id not in 
        (select u.user_id
        from user_table u
            inner join content_creators cc
                on cc.user_id = u.user_id
            inner join video v
                on v.cc_id = cc.cc_id)));



--Q12 

select v.title, v.subtitle, v.video_size, v.views, count(comment_id) as number_of_comments 
from video v
inner join comments c
on v.video_id = c.video_id
where v.video_id in (
    select v.video_id
    from video v
    inner join comments c
        on v.video_id = c.video_id
    group by v.video_id
    having count(comment_id)>=2)
group by v.title, v.subtitle, v.video_size, v.views
order by number_of_comments;

--Q13

/*leftjoin with subquery implemented*/
select *
from (
    select u.user_id, u.first_name, u.last_name,count(try.video_id) as number_of_videos
    from user_table u
    left join (select *
                from content_creators cc
                inner join video v
                on v.cc_id = cc.cc_id) try
    on try.user_id = u.user_id
    group by u.user_id, u.first_name, u.last_name)
order by last_name;

--Q14

/*
using inline view
*/

select compi.cc_id, c.cc_username as username, days_since_latest_upload
from content_creators c join(
    select v.cc_id, trunc(TO_DATE('10-05-2020','MM-DD-YYYY') - max(v.upload_date)) as days_since_latest_upload
    from video v
    group by v.cc_id) compi
    on compi.cc_id = c.cc_id;

