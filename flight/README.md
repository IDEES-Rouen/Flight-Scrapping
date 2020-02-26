
1 - Build docker image

```shell script
export UID=$(id -u)
export GID=$(id -g)
docker-compose build --build-arg UID="$(id -u)" --build-arg GID="$(id -g)"
```

2 - Run Docker container based on step 1

```shell script
sudo docker-compose up -d 
```


