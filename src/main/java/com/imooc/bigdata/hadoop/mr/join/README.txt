MapReduce
Hive:SQl on hadoop
    sql ==> mapreduce/spark explain
    join


    select a.*,b.* from a join b on a.id = b.id

interview：描述如何使用MapReduce来实现join的功能
考察点：
    MapReduce执行流程
    join的底层执行流程
    join的多种实现方式：reducejoin(shuffle) ,mapjoin(没有reduce，也就没有shuffle)

resume:
    从你写的东西开始面试，然后逐步扩展==》你的技能/技术的一个功能链条.不能做简单的api工程师，只会调用别的和只会写crud


reducejoin:
    数据通过mapper加载过来，然后经过shuffle阶段，在Reduce端完成真正的join操作
    a join b on a.deptno = b.deptno  shuffle是根据deptno进行划分的,则reduce得到的既有员工的也有部门的
    因此reducejoin就是一个flag,用于标明该条数据到底是来自哪一个表的

dept  dname ,  deptno
emp  empno,ename,sal,deptno

Q1 Mapper的泛型里面有几个参数,各是什么意思
Mapper<LongWritable, Text, IntWritable,DataInfo>  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
Q2 map方法有几个参数,各是什么意思
map(KEYIN key, VALUEIN value, Context context)
