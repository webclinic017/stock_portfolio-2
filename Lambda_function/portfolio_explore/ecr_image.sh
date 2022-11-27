aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 046034153390.dkr.ecr.us-east-1.amazonaws.com

docker build -t demo-nasdaq-ecr .

docker tag demo-nasdaq-ecr:latest 046034153390.dkr.ecr.us-east-1.amazonaws.com/demo-nasdaq-ecr:latest

docker push 046034153390.dkr.ecr.us-east-1.amazonaws.com/demo-nasdaq-ecr:latest