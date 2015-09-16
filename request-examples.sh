#! /bin/bash

sudo apt-get install -y httpie
http localhost:8088/adm/sparkconf
http localhost:8088/adm/count
http --timeout 60 POST localhost:8088/adm/jsonq < data/query.json

## Test on the browser
## http://localhost:8088/adm/q?zone_latitude=48.858&zone_longitude=2.38115&radius=50000,13000,4000&applist=ZQ331E24,UX196C48,TZ355R94,BJ951G63,QB631X18&timeframe=2&schedule=11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111
