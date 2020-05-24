FROM python:3.6

WORKDIR /var/app

COPY setup.py .
COPY setup.cfg .
COPY requirements.txt .
COPY src/ src/

RUN pip install --trusted-host mirrors.aliyun.com -i http://mirrors.aliyun.com/pypi/simple -r requirements.txt

RUN rm requirements.txt

RUN python setup.py sdist

RUN tar -xzvf dist/DataLoader-*.tar.gz

RUN python DataLoader-*/setup.py install

RUN rm -rf ./*

ENV PYTHONPATH /var/app

CMD ["bash"]