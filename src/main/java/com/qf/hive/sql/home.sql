use home;
set hive.exec.mode.local.auto=true;

drop table student;

create table if not exists student(
    Sid int,
    Sname string,
    Sbirth string,
    Ssex string
)
    row format delimited fields terminated by ' ';

load data local inpath '/opt/data/student.txt' overwrite into table student;
select *
from student;

create table if not exists course(
    Cid int,
    Cname string,
    Tid int
)
    row format delimited fields terminated by ' ';

load data local inpath '/opt/data/course.txt' overwrite into table course;
select *
from course;

create table if not exists sc(
    Sid int,
    Cid int,
    score int
)
    row format delimited fields terminated by ' ';

load data local inpath '/opt/data/sc.txt' overwrite into table sc;
select *
from sc;

create table if not exists teacher(
    tid int,
    tname string
)
    row format delimited fields terminated by ' ';

load data local inpath '/opt/data/teacher.txt' overwrite into table teacher;
select *
from teacher;


-- 1、查询男生、女生人数：
select Ssex, count(*)
from student
group by Ssex;

-- 2、查询出选修一门课程的全部学生的学号和姓名：
select s.Sid, s.Sname
from student s left semi join
(select Sid
from sc group by Sid having count(Sid) == 1 ) sc
on s.Sid = sc.Sid
;

-- 3、查询1981年出生的学生名单
select *
from student where Sbirth like '1981%';

-- 4、查询平均成绩大于80的所有学生的学号、姓名和平均成绩：
select s.sid, s.Sname, avg_score
from student s
         join
     (select sid, avg(score) avg_score
      from sc
      group by Sid
      having avg(score) > 80) sc
     on s.sid = sc.Sid;


-- 5、查询每门课程的平均成绩，结果按平均成绩升序排序，平均成绩相同时，按课程号降序排列：
select c.Cid, c.Cname, sc.avg_score
from course c
         join (select Cid, avg(score) avg_score
               from sc
               group by Cid
               order by avg_score asc, Cid desc
) sc
              on c.Cid = sc.Cid ;

-- 6、查询课程名称为“数学”，且分数低于60的学生名字和分数：
select s.sname, temp.score
from student s join
(select c.cid, sc.score, sc.sid
from course c join sc on c.cid = sc.Cid where c.Cname = "数学" and sc.score < 60) temp
on s.Sid = temp.Sid;

-- 7、查询所有学生的选课情况：
select s.Sname, c.Cname
from student s
         join sc on s.sid = sc.Sid
         join course c on sc.Cid = c.Cid
;

-- 8、查询任何一门课程成绩在70分以上的姓名、课程名称和分数：
select s.Sname, temp.score
from (select Sid, Cid, score
      from sc
      where score > 70) temp
         join student s on temp.Sid = s.Sid
         join course c on temp.Cid = c.Cid;

-- 9、查询01课程比02课程成绩高的所有学生的学号
select t1.sid
from (select Sid, score
      from sc
      where Cid = '01') t1
         join
     (select Sid, score
      from sc
      where Cid = '02') t2
     on t1.Sid = t2.Sid
where t1.score > t2.score;

-- 10、查询平均成绩大于60分的同学的学号和平均成绩
select Sid, avg(score)
from sc
group by Sid
having avg(score) > 60;

-- 11、查询所有同学的学号、姓名、选课数、总成绩
select s.Sid, s.Sname, temp.courseCount, temp.total
from student s
         join
     (select Sid, count(Sid) courseCount, sum(score) total
      from sc
      group by Sid) temp
     on s.Sid = temp.Sid;

-- 12、查询所有课程成绩小于60的同学的学号、姓名：
select s.Sid, s.Sname
from student s
     left semi join
     (select Sid, max(score) maxScore
      from sc
      group by Sid
      having max(score) < 60) temp on s.Sid = temp.Sid;

-- 13、查询没有学全所有课的同学的学号、姓名： ****注意:双重子查询,会报parseException异常 ==> 使用in, not in来代替***
select s.Sid, s.Sname
from student s
         join
     (select sc.Sid, count(Sid)
      from sc
      group by Sid
      having count(Sid) not in (select count(*) from course)) temp
     on s.Sid = temp.Sid;

-- 14、查询至少有一门课与学号为01同学所学相同的同学的学号和姓名：****如果是同一张表进行连接查询，一定要取别名,否则报unSupportException***
select s.Sid, s.Sname
from student s
         join
     (select sc1.Sid
      from sc sc1
      where sc1.Cid in
            (select sc2.Cid
             from sc sc2
             where sc2.Sid = '01')) temp
     on s.Sid = temp.Sid
where s.Sid != '01';

-- 15、查询至少学过学号为01同学所有一门课的其他同学学号和姓名；
select s.Sid, s.Sname
from student s
         join
     (select sc1.Sid
      from sc sc1
      where sc1.Cid in
            (select sc2.Cid
             from sc sc2
             where sc2.Sid = '01')) temp
     on s.Sid = temp.Sid
group by s.Sid
having count(s.Sid) == (select count(*) from sc sc3 where sc3.Sid = '01')
   and s.Sid != '01';




-- add jar 将jar包加入到hive的classpath中，注意不能使用引号
add jar /opt/jar/udf.jar;

-- 创建临时函数名,传入刚才新建的函数全路径名
create temporary function  selectAge as 'com.qf.hive.function.AgeUDF';

-- 测试当前函数
select selectAge('1997-09-12');



-- add jar 将jar包加入到hive的classpath中,在本次会话有效
add jar /opt/jar/utf.jar;

-- 创建临时函数名,传入刚才的函数全路径名
create temporary function copyLetter as 'com.qf.hive.function.ParseMapUDTFNumber';

-- 测试当前函数
select copyLetter("1,v,i,o");

drop temporary function if exists copyLetter;








set hive.exec.mode.local.auto=true;

-- 解析json字符串
add jar /opt/jar/udaf.jar;
create temporary function key2Value as 'com.qf.hive.function.Key2Value';
select key2Value("sex=1&height=180&weight=130&sal=28000", "weight");

add jar /opt/jar/udaf.jar;
create temporary function key2ValueHome as 'com.qf.hive.function.home.Key2ValueHome';
select key2ValueHome("name-zhang$age-30$address-shenzhen", "address");


-- 解析日志
add jar /opt/jar/udaf1.jar;
create temporary function LogRegParser as 'com.qf.hive.function.LogRegParser';
select LogRegParser("220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] \"GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\" 200 8784 \"-\" \"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\"");


set hive.exec.mode.local.auto=true;
add jar /opt/jar/udaf2.jar;
create temporary function sumValue1 as 'com.qf.hive.function.home.UDAFSum_Sample';
select sumValue1(id) from definefunction.stu;

drop temporary function sumValue1;
































