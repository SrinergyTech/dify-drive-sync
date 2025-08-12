FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt
COPY main.py /app/
ENV PORT=8080
CMD ["gunicorn", "-b", ":8080", "main:app"]
