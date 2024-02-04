FROM python:3.11
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

EXPOSE 80
ENTRYPOINT [ "streamlit", "run", "app.py" ]