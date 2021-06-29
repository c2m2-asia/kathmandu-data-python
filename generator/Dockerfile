FROM python:3

COPY . /home/generator/

WORKDIR /home/generator/

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "workers.py"]
CMD ["python3", "business.py"]
