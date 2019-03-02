Readme


Command for execution each problem.

1st Question : File : MutualFriend.java
hadoop jar <jar file location> <ClassName> <Path of soc-LiveJournal1Adj.txt> <Output file path>


2nd Question : File : MutualFriendTop.java
hadoop jar <jar file location> <ClassName> <Path of soc-LiveJournal1Adj.txt> <Output file path>


3rd Question : File : MutualFriendInMemoryJoin.java
hadoop jar <jar file location> <ClassName> <UserA> <UserB> <Path of soc-LiveJournal1Adj.txt> <Path userdata.txt>  <Output file path>


4th Question : File : MutualFriendReduceSideJoin.java
hadoop jar <jar file location> <ClassName> <Path of soc-LiveJournal1Adj.txt> <Path userdata.txt>  <Output file path>


For question 4, after getting final output from map-reduce in folder <output>/final
Run this command : hadoop fs -cat <Path of file part-r-00000> |tail -n 15 > <Path of the output file you need> to get tail 15 output from file. 
Eg : hadoop fs -cat /Users/nikita/output/final/part-r-00000|tail -n 15 > finaloutput_ques4.txt