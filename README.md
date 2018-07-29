# testYarn
1、实现了一个yarn的client端和appliationMaster端，可以申请container打印hello world。

2、运行步骤：
   将ApplicationMaster.java打包成jar包，运行Client。目前的实现中将ApplicationMaster.jar路径写死到Client.java中。

3、client端的实现参考了https://github.com/fireflyc/learnyarn， applicationMaster端的实现参考了hadoop的distributedShell。

