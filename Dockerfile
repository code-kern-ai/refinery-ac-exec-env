FROM kernai/refinery-parent-images:parent-image-updates-v1.22.0-exec-env

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY requirements.txt .

COPY . .

ENTRYPOINT ["/run.sh"] 
