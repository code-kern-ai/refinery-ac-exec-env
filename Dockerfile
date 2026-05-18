FROM registry.dev.kern.ai/code-kern-ai/refinery-parent-images:parent-image-updates-exec-env

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY requirements.txt .

COPY . .

ENTRYPOINT ["/run.sh"] 
