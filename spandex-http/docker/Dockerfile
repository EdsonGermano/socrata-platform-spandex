FROM socrata/java

# Service specific settings.
ENV SPANDEX_SECONDARY_ES_PORT 9300
ENV SPANDEX_SECONDARY_ES_CLUSTER_NAME spandex
ENV SPANDEX_SECONDARY_ES_INDEX_ALIAS spandex
ENV SERVER_NAME spandex-http

# Some paths/locations we care about.
ENV SERVER_ROOT      /srv/${SERVER_NAME}/
ENV SERVER_CONFIG    ${SERVER_NAME}.conf
ENV SERVER_ARTIFACT  ${SERVER_NAME}-assembly.jar

# Environment settings.
ENV JAVA_XMX 512m

# Add other required resources to the container
EXPOSE 8042

ADD ship.d /etc/ship.d
ADD ${SERVER_CONFIG}.j2 /etc/
ADD ${SERVER_ARTIFACT} ${SERVER_ROOT}
