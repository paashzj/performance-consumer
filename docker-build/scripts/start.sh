cd "$(dirname "$0")"

cd ..

echo `pwd`

mkdir /opt/sh/logs

java -Xmx1G -Xms1G -XX:MaxDirectMemorySize=2G -jar /opt/sh/pf-consumer.jar >/opt/sh/logs/console.log 2>/opt/sh/logs/error.log