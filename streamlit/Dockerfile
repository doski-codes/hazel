FROM python:3.11-slim

WORKDIR /app

COPY ./streamlit/app.py /app/app.py
COPY requirements.txt /app/requirements.txt

RUN python3 -m pip install --upgrade pip
RUN pip3 install --no-cache-dir -r /app/requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
