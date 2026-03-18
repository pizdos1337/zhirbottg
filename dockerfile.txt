RUN apt-get update && apt-get install -y \
    fonts-noto-color-emoji \
    fonts-noto-cjk \
    fonts-dejavu-core \
    && fc-cache -f -v
