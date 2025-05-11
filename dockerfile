

FROM python:3.11-slim


WORKDIR /app


COPY generate_data.py /app/
COPY requirements.txt /app/


RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "generate_data.py"]