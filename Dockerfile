FROM scratch

COPY faultwire /faultwire

EXPOSE 8000 8081

ENTRYPOINT ["/faultwire"]