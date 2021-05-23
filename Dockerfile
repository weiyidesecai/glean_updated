FROM python

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt --no-cache-dir

RUN apt-get update && apt-get install -y \
  default-jre-headless \
  && rm -rf /var/lib/apt/lists/*

COPY . ./
ENTRYPOINT [ "python" ]
CMD [ "invoice.py" ]
/Users/han.cai@ibm.com/Documents/cxx简历/glean/glean/Dockerfile