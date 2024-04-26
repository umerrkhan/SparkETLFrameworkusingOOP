sqlstatment = "\
select 7839 as EMPNO, 'KING' as ENAME,'PRESIDENT'as JOB,null as mgr, '17-11-1981'as HIREDATE, 5000 as sal, null as comm, 10 as DPEPTNO  \
union all \
select 7698 as EMPNO, 'BLAKE' as ENAME,  'MANAGER'as JOB,7839 as mgr, '1-5-1981'as HIREDATE, 2850 as sal, null as comm, 30 as DPEPTNO \
union all \
select 7839 as EMPNO, 'KING' as ENAME,'PRESIDENT'as JOB,null as mgr, '17-11-1981'as HIREDATE, 5000 as sal, null as comm, 10 as DPEPTNO \
union all \
select 7698 as EMPNO, 'BLAKE' as ENAME,  'MANAGER'as JOB,7839 as mgr, '1-5-1981'as HIREDATE, 2850 as sal, null as comm, 30 as DPEPTNO \
union all \
select 7782 as EMPNO, 'CLARK' as ENAME,  'MANAGER'as JOB,7839 as mgr, '9-6-1981'as HIREDATE, 2450 as sal, null as comm, 10 as DPEPTNO \
union all \
select 7566 as EMPNO, 'JONES' as ENAME,  'MANAGER'as JOB,7839 as mgr, '2-4-1981'as HIREDATE, 2975 as sal, null as comm, 20 as DPEPTNO \
union all \
select 7788 as EMPNO, 'SCOTT' as ENAME,  'ANALYST'as JOB,7566 as mgr, '13-JUL-87'as HIREDATE,3000 as sal, null as comm, 20 as DPEPTNO \
union all \
select 7902 as EMPNO, 'FORD' as ENAME,'ANALYST'as JOB,7566 as mgr, '3-12-1981'as HIREDATE, 3000 as sal, null as comm, 20 as DPEPTNO \
union all \
select 7369 as EMPNO, 'SMITH' as ENAME,  'CLERK'as JOB,7902 as mgr, '17-12-1980'as HIREDATE, 800 as sal, null as comm, 20 as DPEPTNO \
union all \
select 7499 as EMPNO, 'ALLEN' as ENAME,  'SALESMAN'as JOB,7698 as mgr, '20-2-1981'as HIREDATE,1600 as sal,  300 as comm, 30 as DPEPTNO \
union all \
select 7521 as EMPNO, 'WARD' as ENAME,'SALESMAN'as JOB,7698 as mgr, '22-2-1981'as HIREDATE, 1250 as sal,  500 as comm, 30 as DPEPTNO \
union all \
select 7654 as EMPNO, 'MARTIN'as ENAME, 'SALESMAN'as JOB,7698 as mgr, '28-9-1981'as HIREDATE, 1250 as sal, 1400 as comm, 30 as DPEPTNO \
union all \
select 7844 as EMPNO, 'TURNER'as ENAME, 'SALESMAN'as JOB,7698 as mgr, '8-9-1981'as HIREDATE, 1500 as sal, 0 as comm, 30 as DPEPTNO \
union all \
select 7876 as EMPNO, 'ADAMS' as ENAME,  'CLERK'as JOB,  7788 as mgr, '13-JUL-87'as HIREDATE,1100 as sal, null as comm, 20 as DPEPTNO \
union all \
select 7900 as EMPNO, 'JAMES' as ENAME,  'CLERK'as JOB,  7698 as mgr, '3-12-1981'as HIREDATE,  950 as sal, null as comm, 30 as DPEPTNO \
union all \
select 7934 as EMPNO, 'MILLER'as ENAME, 'CLERK'as JOB,  7782 as mgr, '23-1-1982'as HIREDATE, 1300 as sal, null as comm, 10 as DPEPTNO \
"
dfSegments = spark.sql(sqlstatment)
#dfSegments = dfSegments.groupBy("DPEPTNO").pivot("JOB").sum("sal")
dfSegments = dfSegments.groupBy("JOB").pivot("JOB").sum("DPEPTNO")
display(dfSegments)