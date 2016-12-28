@echo off

setlocal

SET libDir=%~dp0..\linden-core\target\lib
SET distDir=%~dp0..\linden-core\target

if "%1" == "" (
	echo Usage: bin\start-linden-server.cmd config-dir
	goto :eof
)

SET JAVA_OPTS="-server -d64"
SET HEAP_OPTS="-Xmx1g -Xms1g -XX:NewSize=256m"
SET MAIN_CLASS="com.xiaomi.linden.service.LindenServer"
SET CLASSPATH=%CLASSPATH%;%libDir%\*;%distDir%\*
call java "%JAVA_OPTS%" "%HEAP_OPTS%" -classpath "%CLASSPATH%" %MAIN_CLASS% %1

endlocal