cd "$(dirname "$0")"

cd ..

echo `pwd`

mkdir /opt/sh/logs

java -Xmx1G -Xms1G -XX:MaxDirectMemorySize=2G -jar /opt/sh/pf-consumer.jar >>/opt/sh/logs/stdout.log 2>>/opt/sh/logs/stderr.log
