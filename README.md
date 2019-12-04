Скрипт предназначен для парсинга логов nginx.

В логах nginx.conf выставлен формат custom:

log_format  custom  '$remote_addr - $remote_user [$time_local] "$request" '
                    '$status $upstream_response_time $body_bytes_sent "$http_referer" '
                    '"$http_user_agent" "$http_x_forwarded_for" "$upstream_addr"';
