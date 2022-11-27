FROM amazon/aws-lambda-python:3.8

RUN python3 -m pip install --upgrade pip

COPY requirements.txt .

RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY portfolio.py ${LAMBDA_TASK_ROOT}

CMD ["portfolio.handler"]