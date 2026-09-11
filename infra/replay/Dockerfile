FROM nginxinc/nginx-unprivileged:stable-alpine@sha256:442753882674b49ae2c1de83ed67896131c0777f56df5005e356e62bc3f7e7ce

# The build context contains only the already-built, reviewed public replay.
COPY nginx.conf /etc/nginx/nginx.conf
COPY site/ /srv/payops/
USER 1000:1000
EXPOSE 7860
# Bypass image entrypoint scripts: this server needs no runtime configuration writes.
ENTRYPOINT []
CMD ["nginx", "-g", "daemon off;"]
