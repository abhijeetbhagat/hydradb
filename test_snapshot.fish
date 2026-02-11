#!/opt/homebrew/bin/fish

killall server 2>/dev/null

cargo build -p server --release

if test -e leader
  rm -rf leader
  mkdir leader
end
if test -e follower1
  rm -rf follower1
  mkdir follower1
end

cp target/release/server leader/
cp target/release/server follower1/

set -x RUST_LOG info,actix_server=warn,reqwest=warn

cd leader
echo "starting Leader..."
./server --namespace test --id 1 --port 9896 --logs-per-snapshot 10 --snapshot-retention 0 > leader.log 2>&1 &
sleep 1
lsof -ti tcp:9896

curl -s 'http://localhost:9896/init' -X POST -H "Content-Type: application/json" -d '[]'

echo "writing 10 kv pairs to trigger snapshots ..."
for i in (seq 1 50)
    curl -s 'http://localhost:9896/write' -X POST -H "Content-Type: application/json" \
    -d "{\"Put\": {\"key\": \"key-$i\", \"value\": \"value-$i\"}}" > /dev/null
end
echo "data written"

echo "waiting for purge..."
while true
    set METRICS (curl -s 'http://localhost:9896/metrics')
    if string match -q '*"purged":{*' -- $METRICS
        echo "logs purged"
        break
    end
    sleep 1
end

cd ../follower1
echo "starting follower1"
./server --namespace test --id 2 --port 9897 > follower.log 2>&1 &
sleep 1
lsof -ti tcp:9897

echo "Adding Learner..."
curl -s 'http://localhost:9896/add-learner' -X POST -H "Content-Type: application/json" \
-d '[2, "127.0.0.1:9897"]'

sleep 5 # give time for snapshot transfer

echo "reading key-1 from follower1 ..."
set RESPONSE (curl -s 'http://localhost:9897/read' -X POST -H "Content-Type: application/json" -d '"key-1"')

if string match -q "*value-1*" -- $RESPONSE
    echo "snapshot working as expected"
else
    echo "snapshot failed: $RESPONSE"
end

echo "Checking logs for Snapshot activity..."
grep -i "snapshot" ../leader/leader.log
