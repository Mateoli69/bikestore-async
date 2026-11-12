#!/usr/bin/env bash
# Envía 10 pedidos de prueba al API "Ordenar"
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/ordenar \
    -H "Content-Type: application/json" \
    -d "{\"cliente\":\"Cliente $i\",\"email\":\"cliente$i@correo.com\",\"total\":$((100 + i * 25)).90}"
  echo
done
