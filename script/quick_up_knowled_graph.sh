#run from root of project
mkdir "data"
docker-compose up &
echo sleep 5s
sleep 5
echo run quick build extract data
python3 quick_run/build_knowledge_graph.py
