FROM kernai/refinery-parent-images:v1.14.0-exec-env

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY requirements.txt .

COPY . .

ENTRYPOINT ["/run.sh"] 
