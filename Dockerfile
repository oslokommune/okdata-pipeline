FROM public.ecr.aws/lambda/python:3.13

RUN dnf install gcc -y

COPY okdata ${LAMBDA_TASK_ROOT}/okdata
COPY README.md ${LAMBDA_TASK_ROOT}
COPY setup.py ${LAMBDA_TASK_ROOT}
# This is needed to declare `okdata.pipeline` as a namespace
# package. Dependencies are still locked with requirements.txt further
# down.
RUN pip install --no-deps .

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

CMD ["set-me-in-serverless.yaml"]
