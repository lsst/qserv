#!/bin/sh

# Wait for the dashboard configuration folder to be populated by the init container.

until [ -f /config-etc/goahead ];
do
    sleep 1
done
