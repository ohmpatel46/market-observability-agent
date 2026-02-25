FROM python:3.12-slim

WORKDIR /worker

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY main.py ./main.py

CMD ["python", "main.py"]