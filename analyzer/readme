# Install all requirement libraries using pip
pip install -r requirement.txt



# -----  Data Analyzer  -----
# run the flask application using the following command
python3 ./analyzer/vesselzone.py

# to build docker image
docker build --platform linux/amd64 -t azzulhisham/py-tss-analyzer-linux:v1.20 -f Dockerfile_analyzer .  

# push image to docker hub
docker push azzulhisham/py-tss-analyzer-linux:v1.20

# deploy to kubernetes
kubectl apply -f deployment_analyzer.yaml -n system-pnav

