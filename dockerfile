FROM apache/airflow:3.1.3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python3 -c "import duckdb; con = duckdb.connect(); con.install_extension('httpfs'); con.load_extension('httpfs'); con.close()"