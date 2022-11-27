aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 046034153390.dkr.ecr.us-east-1.amazonaws.com

docker build -t risk-report-lambda .

docker tag risk-report-lambda:latest 046034153390.dkr.ecr.us-east-1.amazonaws.com/risk-report-lambda:latest

docker push 046034153390.dkr.ecr.us-east-1.amazonaws.com/risk-report-lambda:latest