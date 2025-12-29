#!/opt/homebrew/bin/fish

killall server

cargo build -p server

if test -e leader
  rm -rf leader
  mkdir leader
end
if test -e follower1
  rm -rf follower1
  mkdir follower1
end
if test -e follower2
  rm -rf follower2
  mkdir follower2
end

cp target/debug/server leader/
cp target/debug/server follower1/
cp target/debug/server follower2/

cd leader
./server --namespace test --id 1 --port 9896 > /dev/null 2&>1 &
sleep 1

cd ../follower1
./server --namespace test --id 2 --port 9897 > /dev/null 2&>1 &
sleep 1

cd ../follower2
./server --namespace test --id 3 --port 9898 > /dev/null 2&>1 &
sleep 1

curl 'http://localhost:9896/init' -X POST -H "Content-Type: application/json" --data '[]' 
curl 'http://localhost:9896/add-learner' -X POST -H "Content-Type: application/json" --data '[2, "127.0.0.1:9897"]'
curl 'http://localhost:9896/add-learner' -X POST -H "Content-Type: application/json" --data '[3, "127.0.0.1:9898"]'
curl 'http://localhost:9896/change-membership' -X POST -H "Content-Type: application/json" --data '[1,2,3]'

