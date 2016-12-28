@echo off

setlocal

SET CLASSPATH=%CLASSPATH%;%~dp0..\linden-core\target\lib\*

echo on

call java -cp "%CLASSPATH%" org.apache.zookeeper.server.quorum.QuorumPeerMain %~dp0..\config\zookeeper.properties

endlocal
