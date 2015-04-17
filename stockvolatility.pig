REGISTER src/volatility.jar
REGISTER src/ComputeXi.jar;

A = LOAD 'hdfs:///pigdata/*.csv' using PigStorage(',','-tagFile'); 
X = FILTER A by $1 !='null';
B = FOREACH X GENERATE $0 as FileName, $1 as Date, $7 as ClosingBalance;
C = FILTER B by Date !='Date';
D = FOREACH C GENERATE FileName, FLATTEN(STRSPLIT(Date,'-',3)) as (Year,Month,Day),ClosingBalance; 
E = GROUP D by (FileName,Year,Month);
F = FOREACH E GENERATE pigTest.COMPUTEXI($1) as Param:chararray;
G = FOREACH F GENERATE FLATTEN(STRSPLIT(Param,'\t'))as (FName,Xi:double);
H = GROUP G by FName;
I = FOREACH H GENERATE volatilityCompute.VOLATILITY($1) as P1:chararray;
J = FOREACH I GENERATE FLATTEN(STRSPLIT(P1,'\t'))as (FN,Vol);
FILT = FILTER J by FN!='null';
L = FOREACH FILT GENERATE FN, (double)Vol;
M = ORDER L by Vol ASC;
N = LIMIT M 10;
O = ORDER L by Vol DESC;
P = LIMIT O 10;
FINAL = UNION P,N;

STORE FINAL INTO 'hdfs:///pigdata/hw3_out';

